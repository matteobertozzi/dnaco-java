package tech.dnaco.net.pubsub;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
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
import tech.dnaco.net.pubsub.LogFileUtil.LogsTracker;
import tech.dnaco.net.pubsub.LogFileUtil.LogsTrackerSupplier;
import tech.dnaco.net.util.ByteBufIntUtil;
import tech.dnaco.net.util.FileUtil;

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
      Logger.debug("SERVER-RECV: {}", frame);

      final ByteBuf packet = frame.getData();
      final int head = packet.readByte() & 0xff;
      final int type = (head >> 5) & 7;
      Logger.debug(" -> HEAD:" + Integer.toBinaryString(head) + " TYPE:" + type);
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

    private void processPublish(final ChannelHandlerContext ctx, final ByteBuf topic, final long offset, final ByteBuf data) {
      Logger.debug("OFFSET:" + offset
        + " TOPIC:" + topic.toString(StandardCharsets.UTF_8)
        //+ " DATA:" + data.toString(StandardCharsets.UTF_8)
      );

      ByteBuf result = null;
      try {
        for (final LogSyncServiceListener listener: listeners) {
          listener.publish(topic, offset, data);
        }

        // PUBACK
        // +------------+-----------------+-------+
        // | 001 000 TT | topic-len-bytes | topic |
        // +------------+-----------------+-------+
        final int topicLen = topic.readableBytes();
        final int topicLenBytes = IntUtil.size(topicLen);
        result = ctx.alloc().buffer();
        result.writeByte((1 << 5) | (topicLenBytes - 1));
        ByteBufIntUtil.writeFixed(result, topicLen, topicLenBytes);
        result.writeBytes(topic.slice());
        Logger.debug("send ack");
      } catch (final LogResetOffset e) {
        // PUBNAK RESET
        // +------------+-----------------+-------+---------------+
        // | 001 001 TT | topic-len-bytes | topic | reset-voffset |
        // +------------+-----------------+-------+---------------+
        final int topicLen = topic.readableBytes();
        final int topicLenBytes = IntUtil.size(topicLen);
        result = ctx.alloc().buffer();
        result.writeByte((1 << 5) | (1 << 2) | (topicLenBytes - 1));
        ByteBufIntUtil.writeFixed(result, topicLen, topicLenBytes);
        result.writeBytes(topic.slice());
        ByteBufIntUtil.writeVarLong(result, e.getOffset());
        Logger.debug("send nak/reset");
      } catch (final Throwable e) {
        Logger.error(e, "unable to publish topic {} offset {}", topic.toString(StandardCharsets.UTF_8), offset);

        // PUBNAK Failure
        // +------------+-----------------+-------+
        // | 001 010 TT | topic-len-bytes | topic |
        // +------------+-----------------+-------+
        final int topicLen = topic.readableBytes();
        final int topicLenBytes = IntUtil.size(topicLen);
        result = ctx.alloc().buffer();
        result.writeByte((1 << 5) | (1 << 3) | (topicLenBytes - 1));
        ByteBufIntUtil.writeFixed(result, topicLen, topicLenBytes);
        result.writeBytes(topic.slice());
        Logger.debug("send nak");
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

    private final LogsTrackerSupplier logsTrackerSupplier;

    public LogSyncServiceStoreHandler(final LogsTrackerSupplier logsTrackerSupplier) {
      this.logsTrackerSupplier = logsTrackerSupplier;
    }

    public LogsTracker getTopicSequence(final String topic) {
      return logsTrackerSupplier.getLogsTracker(topic);
    }

    @Override
    public void publish(final ByteBuf topic, final long offset, final ByteBuf data) throws IOException {
      final LogsTracker seq = getTopicSequence(topic.toString(StandardCharsets.UTF_8));

      if (seq.getMaxOffset() < offset) {
        Logger.warn("missing data, expected offset {} got {}", seq.getMaxOffset(), offset);
        throw new LogResetOffset(seq.getMaxOffset());
      }

      final long dataLength = data.readableBytes();

      File logFile = seq.getLastBlockFile();
      if (logFile == null || logFile.length() > ROLL_SIZE) {
        logFile = seq.addNewFile();
      }

      Logger.debug(" -> " + logFile + " -> " + logFile.exists() + " -> " + offset);
      if (logFile.exists()) {
        try (FileChannel channel = FileUtil.openFile(logFile.toPath())) {
          final long fileOffset = logFile.length();
          FileUtil.write(channel, fileOffset, data);
        }
      } else {
        logFile.getParentFile().mkdirs();
        try (FileChannel channel = FileUtil.createEmptyFile(logFile.toPath())) {
          FileUtil.write(channel, 0, data);
        }
      }

      seq.addData(dataLength);
    }
  }
}
