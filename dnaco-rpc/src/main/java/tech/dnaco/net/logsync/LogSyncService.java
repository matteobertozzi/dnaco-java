package tech.dnaco.net.logsync;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import tech.dnaco.bytes.encoding.IntUtil;
import tech.dnaco.logging.Logger;
import tech.dnaco.net.AbstractService;
import tech.dnaco.net.frame.DnacoFrame;
import tech.dnaco.net.frame.DnacoFrameDecoder;
import tech.dnaco.net.frame.DnacoFrameEncoder;
import tech.dnaco.net.logsync.LogFileUtil.LogsFileTracker;
import tech.dnaco.net.logsync.LogFileUtil.LogsTrackerSupplier;
import tech.dnaco.net.util.ByteBufIntUtil;
import tech.dnaco.net.util.FileUtil;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.strings.HumansUtil.HumanLongValueConverter;
import tech.dnaco.telemetry.TelemetryCollector;
import tech.dnaco.telemetry.TelemetryCollectorData;

public class LogSyncService extends AbstractService {
  private final LogSyncServiceHandler handler;

  public LogSyncService() {
    this.handler = new LogSyncServiceHandler();
  }

  @Override
  protected void setupPipeline(final ChannelPipeline pipeline) {
    pipeline.addLast(new DnacoFrameDecoder());
    pipeline.addLast(new DnacoFrameEncoder());
    pipeline.addLast(handler);
  }

  public void registerListener(final LogSyncServiceListener listener) {
    handler.listeners.add(listener);
  }

  @Sharable
  private static final class LogSyncServiceHandler extends ServiceChannelInboundHandler<DnacoFrame> {
    private final CopyOnWriteArrayList<LogSyncServiceListener> listeners = new CopyOnWriteArrayList<>();

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final DnacoFrame frame) throws Exception {
      //Logger.trace("SERVER-RECV: {}", frame);

      final ByteBuf packet = frame.getData();
      final int head = packet.readByte() & 0xff;
      final int type = (head >> 5) & 7;
      //Logger.trace(" -> HEAD:" + Integer.toBinaryString(head) + " TYPE:" + type);
      switch (type) {
        case 0: {
          // PUBLISH
          // +------------+--------+-----------------+
          // | 000 OOO TT | offset | topic-len-bytes |
          // +------------+--------+-----------------+
          // |    topic   |           data           |
          // +------------+--------------------------+
          final long offset = ByteBufIntUtil.readFixed(packet, 1 + ((head >> 2) & 7));
          final int topicLen = (int) ByteBufIntUtil.readFixed(packet, 1 + (head & 3));
          final ByteBuf topic = packet.readSlice(topicLen);
          final ByteBuf data = packet.slice();
          processPublish(ctx, topic, offset, data);
          break;
        }
      }
    }

    private void processPublish(final ChannelHandlerContext ctx, final ByteBuf rawLogsId, final long offset, final ByteBuf data) {
      final String logsId = rawLogsId.toString(StandardCharsets.UTF_8);
      //Logger.trace("{} {offset} {data}", logsId, offset, data.toString(StandardCharsets.UTF_8));

      ByteBuf result = null;
      try {
        for (final LogSyncServiceListener listener: listeners) {
          listener.publish(rawLogsId, offset, data);
        }

        // PUBACK
        // +------------+-----------------+-------+
        // | 001 000 TT | topic-len-bytes | topic |
        // +------------+-----------------+-------+
        final int topicLen = rawLogsId.readableBytes();
        final int topicLenBytes = IntUtil.size(topicLen);
        result = ctx.alloc().buffer();
        result.writeByte((1 << 5) | (topicLenBytes - 1));
        ByteBufIntUtil.writeFixed(result, topicLen, topicLenBytes);
        result.writeBytes(rawLogsId.slice());
        Logger.debug("{} send ACK {offset}", logsId, offset);
      } catch (final LogResetOffset e) {
        // PUBNAK RESET
        // +------------+-----------------+-------+---------------+
        // | 001 001 TT | topic-len-bytes | topic | reset-voffset |
        // +------------+-----------------+-------+---------------+
        final int topicLen = rawLogsId.readableBytes();
        final int topicLenBytes = IntUtil.size(topicLen);
        result = ctx.alloc().buffer();
        result.writeByte((1 << 5) | (1 << 2) | (topicLenBytes - 1));
        ByteBufIntUtil.writeFixed(result, topicLen, topicLenBytes);
        result.writeBytes(rawLogsId.slice());
        ByteBufIntUtil.writeVarLong(result, e.getOffset());
        Logger.debug("{} send NAK/RESET {offset} {resetOffset}", logsId, offset, e.getOffset());
      } catch (final Throwable e) {
        // PUBNAK Failure
        // +------------+-----------------+-------+
        // | 001 010 TT | topic-len-bytes | topic |
        // +------------+-----------------+-------+
        final int topicLen = rawLogsId.readableBytes();
        final int topicLenBytes = IntUtil.size(topicLen);
        result = ctx.alloc().buffer();
        result.writeByte((1 << 5) | (1 << 3) | (topicLenBytes - 1));
        ByteBufIntUtil.writeFixed(result, topicLen, topicLenBytes);
        result.writeBytes(rawLogsId.slice());
        Logger.debug(e, "{} send NAK. unable to publish {offset}", logsId, offset);
      } finally {
        ctx.writeAndFlush(DnacoFrame.alloc(0, result));
      }
    }

    @Override
    protected AbstractServiceSession sessionConnected(final ChannelHandlerContext ctx) {
      // no-op
      return null;
    }

    @Override
    protected void sessionDisconnected(final AbstractServiceSession session) {
      // no-op
    }
  }

  public interface LogSyncServiceListener {
    void publish(ByteBuf topic, long offset, ByteBuf data) throws IOException;
  }

  private static final class LogResetOffset extends IOException {
    private final long offset;

    public LogResetOffset(final long offset) {
      this.offset = offset;
    }

    public long getOffset() {
      return offset;
    }
  }

  public static class LogSyncServiceStoreHandler implements LogSyncServiceListener {
    private static final long ROLL_SIZE = 32 << 20;

    private final LogSyncServiceStats stats = new TelemetryCollector.Builder()
      .setName("log_sync_service")
      .setLabel("Log Sync Service")
      .setUnit(HumansUtil.HUMAN_COUNT)
      .register(new LogSyncServiceStats());

    private final LogsTrackerSupplier logsTrackerSupplier;

    public LogSyncServiceStoreHandler(final LogsTrackerSupplier logsTrackerSupplier) {
      this.logsTrackerSupplier = logsTrackerSupplier;
    }

    public LogsFileTracker getTopicSequence(final String topic) {
      return logsTrackerSupplier.getLogsTracker(topic);
    }

    @Override
    public void publish(final ByteBuf topic, final long offset, final ByteBuf data) throws IOException {
      final LogsFileTracker seq = getTopicSequence(topic.toString(StandardCharsets.UTF_8));

      if (seq.getMaxOffset() < offset) {
        Logger.warn("{} missing data, expected {maxOffset} got {offset}", seq.getLogsId(), seq.getMaxOffset(), offset);
        throw new LogResetOffset(seq.getMaxOffset());
      }

      final long dataLength = data.readableBytes();

      File logFile = seq.getLastBlockFile();
      if (logFile == null || logFile.length() > ROLL_SIZE) {
        Logger.debug("{} roll {logFile} {logSize}", seq.getLogsId(), logFile, logFile != null ? logFile.length() : 0);
        logFile = seq.addNewFile();
      }

      if (logFile.exists()) {
        try (FileChannel channel = FileUtil.openFile(logFile.toPath())) {
          final long fileOffset = logFile.length();
          Logger.trace("{} {} APPEND {offset} {fileOffset}", seq.getLogsId(), logFile, offset, fileOffset);
          FileUtil.write(channel, fileOffset, data);
        }
      } else {
        logFile.getParentFile().mkdirs();
        try (FileChannel channel = FileUtil.createEmptyFile(logFile.toPath())) {
          Logger.trace("{} {} NEW FILE {offset}", seq.getLogsId(), logFile, offset);
          FileUtil.write(channel, 0, data);
        }
      }
      seq.addData(dataLength);
      stats.addReceived(seq.getLogsId(), dataLength);
    }
  }

  private static final class LogSyncServiceStats implements TelemetryCollector, TelemetryCollectorData {
    private final HashMap<String, LogState> states = new HashMap<>();

    @Override
    public String getType() {
      return "LOGS_SYNC";
    }

    @Override
    public TelemetryCollectorData getSnapshot() {
      return this;
    }

    public void addReceived(final String logsId, final long dataLength) {
      states.computeIfAbsent(logsId, (k) -> new LogState()).add(dataLength);
    }

    @Override
    public StringBuilder toHumanReport(final StringBuilder report, final HumanLongValueConverter humanConverter) {
      final long now = System.nanoTime();
      final ArrayList<String> logs = new ArrayList<>(states.keySet());
      Collections.sort(logs);
      for (final String logsId: logs) {
        final LogState state = states.get(logsId);
        report.append("\n");
        report.append(logsId).append(":");
        report.append(" received:").append(HumansUtil.humanSize(state.receivedSize));
        report.append(" lastReceived:").append(HumansUtil.humanTimeNanos(now - state.timestamp));
      }
      report.append("\n");
      return report;
    }

    private static final class LogState {
      private long receivedSize;
      private long timestamp;

      public void add(final long dataLength) {
        this.receivedSize += dataLength;
        this.timestamp = System.nanoTime();
      }
    }
  }
}
