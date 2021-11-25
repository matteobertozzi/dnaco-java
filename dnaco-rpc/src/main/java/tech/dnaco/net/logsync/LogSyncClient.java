package tech.dnaco.net.logsync;

import java.io.File;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FileRegion;
import io.netty.channel.SimpleChannelInboundHandler;
import tech.dnaco.bytes.encoding.IntUtil;
import tech.dnaco.logging.Logger;
import tech.dnaco.net.AbstractClient;
import tech.dnaco.net.ClientEventLoop;
import tech.dnaco.net.ServiceEventLoop;
import tech.dnaco.net.frame.DnacoFrame;
import tech.dnaco.net.frame.DnacoFrameDecoder;
import tech.dnaco.net.logsync.LogFileUtil.LogOffsetStore;
import tech.dnaco.net.logsync.LogFileUtil.LogsConsumer;
import tech.dnaco.net.util.ByteBufIntUtil;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.telemetry.ConcurrentTimeRangeCounter;
import tech.dnaco.telemetry.TelemetryCollector;
import tech.dnaco.telemetry.TelemetryCollectorGroup;
import tech.dnaco.telemetry.TelemetryCollectorRegistry;
import tech.dnaco.time.RetryUtil;
import tech.dnaco.time.TimeUtil;

public class LogSyncClient extends AbstractClient {
  public static final class LogState {
    private final LogsConsumer tracker;
    private final ByteBuf topic;

    private long waiting = 0;
    private int lastSentChunkSize;

    public LogState(final LogsConsumer consumer, final ByteBuf topic, final int lastSentChunkSize) {
      this.tracker = consumer;
      this.topic = topic;
      this.lastSentChunkSize = lastSentChunkSize;
    }

    public void setLastSentChunkSize(final int size) {
      this.lastSentChunkSize = size;
    }

    public LogsConsumer consumer() {
      return this.tracker;
    }

    public ByteBuf topic() {
      return this.topic;
    }

    public void consumeLastChunk(final List<LogOffsetStore> stores) throws Exception {
      final long newOffset = tracker.getOffset() + lastSentChunkSize;
      for (final LogOffsetStore store: stores) {
        store.store(tracker.getLogsId(), newOffset);
      }
      tracker.consume(lastSentChunkSize);
    }

    public void setOffset(final List<LogOffsetStore> stores, final long offset) throws Exception {
      for (final LogOffsetStore store: stores) {
        store.store(tracker.getLogsId(), offset);
      }
      tracker.setOffset(offset);
    }
  }

  private final CopyOnWriteArrayList<LogOffsetStore> offsetStores = new CopyOnWriteArrayList<>();
  private final ConcurrentHashMap<ByteBuf, LogState> topics = new ConcurrentHashMap<>();
  private final CopyOnWriteArrayList<LogState> trackers = new CopyOnWriteArrayList<>();

  protected LogSyncClient(final Bootstrap bootstrap, final RetryUtil.RetryLogic retryLogic) {
    super(bootstrap, retryLogic);
  }

  public LogSyncClient registerOffsetStore(final LogOffsetStore store) {
    this.offsetStores.add(store);
    return this;
  }

  public void add(final LogsConsumer consumer) {
    final ByteBuf logsId = Unpooled.wrappedBuffer(consumer.getLogsId().getBytes());
    final LogState state = new LogState(consumer, logsId, 0);
    topics.put(logsId, state);
    trackers.add(state);
    consumer.registerDataPublishedListener(x -> this.dataPublished(state));
  }

  public static LogSyncClient newTcpClient(final ClientEventLoop eloop, final RetryUtil.RetryLogic retryLogic) {
    return newTcpClient(eloop.getWorkerGroup(), eloop.getChannelClass(), retryLogic);
  }

  public static LogSyncClient newTcpClient(final ServiceEventLoop eloop, final RetryUtil.RetryLogic retryLogic) {
    return newTcpClient(eloop.getWorkerGroup(), eloop.getClientChannelClass(), retryLogic);
  }

  public static LogSyncClient newTcpClient(final EventLoopGroup eloopGroup,
      final Class<? extends Channel> channelClass,
      final RetryUtil.RetryLogic retryLogic) {
    final Bootstrap bootstrap = newTcpClientBootstrap(eloopGroup, channelClass);
    final LogSyncClient client = new LogSyncClient(bootstrap, retryLogic);
    client.setupTcpServerBootstrap();
    return client;
  }

  public static LogSyncClient newUnixClient(final EventLoopGroup eloopGroup,
      final Class<? extends Channel> channelClass,
      final RetryUtil.RetryLogic retryLogic) {
    final Bootstrap bootstrap = newUnixClientBootstrap(eloopGroup, channelClass);
    final LogSyncClient client = new LogSyncClient(bootstrap, retryLogic);
    client.setupUnixServerBootstrap();
    return client;
  }

  @Override
  protected void setupPipeline(final ChannelPipeline pipeline) {
    pipeline.addLast(new DnacoFrameDecoder());
    pipeline.addLast(new LogSyncClientHandler(this));
  }

  private void dataPublished(final LogState state) {
    Logger.debug("data published: {}", state.consumer().getMaxOffset());
    fireEvent(state);
  }

  private long tryPublish(final LogState state) {
    final LogsConsumer consumer = state.consumer();
    if (!consumer.hasMore()) {
      Logger.debug("NOTHING TO PUBLISH");
      return 0;
    }

    final FileRegion fileRegion = getFileRegion(consumer);
    final long offset = consumer.getOffset();
    final int length = (int) fileRegion.count();
    state.setLastSentChunkSize(length);
    Logger.trace("PUBLISH: SEND OFFSET:{} LENGTH:{}", offset, length);
    LogSyncClientStats.getInstance(consumer.getName()).send(offset, length, consumer.getMaxOffset());

    // PUBLISH
    // +------------+--------+-----------------+
    // | --- --- -- | offset | topic-len-bytes |
    // +------------+--------+-----------------+
    // |    topic   |           data           |
    // +------------+--------------------------+
    final ByteBuf topic = state.topic();
    final ByteBuf packet = ByteBufAllocator.DEFAULT.buffer();
    final int topicLen = topic.readableBytes();
    final int topicLenBytes = IntUtil.size(topicLen);
    final int offsetBytes = offset != 0 ? IntUtil.size(offset) : 1;
    packet.writeInt((1 + offsetBytes + topicLenBytes + topicLen + length) - 1);
    packet.writeByte((0 << 5) | ((offsetBytes - 1) << 2) | (topicLenBytes - 1));
    ByteBufIntUtil.writeFixed(packet, offset, offsetBytes);
    ByteBufIntUtil.writeFixed(packet, topicLen, topicLenBytes);
    packet.writeBytes(topic.slice());
    write(packet);
    writeAndFlush(fileRegion);
    return System.nanoTime();
  }

  private DefaultFileRegion getFileRegion(final LogsConsumer consumer) {
    final File logFile = consumer.getBlockFile();
    final long logOffset = consumer.getBlockOffset();
    final long logAvail = consumer.getBlockAvailable();
    final long count = Math.min(logAvail, 1 << 20);
    Logger.debug("TRY PUBLISH REGION {} logOff={} logAvail={} count={} length={}",
      logFile, logOffset, logAvail, count, logFile.length());
    if (logOffset + logAvail >= logFile.length()) {
      final long newCount = Math.min(1 << 20, logFile.length() - logOffset);
      if (newCount != count) {
        Logger.error("WRONG LOG-REGION OFFSET {} logOff={} logAvail={} count={} length={}",
          logFile, logOffset, logAvail, count, logFile.length());
      }
    }

    return new DefaultFileRegion(logFile, logOffset, count);
  }

  private Consumer<LogSyncClient> connectedHandler;
  public LogSyncClient whenConnected(final Consumer<LogSyncClient> consumer) {
    this.connectedHandler = consumer;
    return this;
  }

  private static final class LogSyncClientHandler extends SimpleChannelInboundHandler<DnacoFrame> {
    private final LogSyncClient client;

    public LogSyncClientHandler(final LogSyncClient client) {
      this.client = client;
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
      client.setState(ClientState.CONNECTED);
      Logger.debug("channel registered: {}", ctx.channel().remoteAddress());
      if (client.connectedHandler != null) {
        client.connectedHandler.accept(client);
      } else {
        client.setReady();
      }
      ctx.fireChannelActive();
      for (final LogState state: client.trackers) {
        tryPublish(state);
      }
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
      client.setState(ClientState.DISCONNECTED);
      Logger.debug("channel unregistered: {}", ctx.channel().remoteAddress());
      ctx.fireChannelInactive();
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final DnacoFrame frame) throws Exception {
      Logger.debug("CLIENT-RECV: " + frame);
      final ByteBuf packet = frame.getData();
      final int head = packet.readByte() & 0xff;
      final int type = (head >> 5) & 7;
      switch (type) {
        case 0: break;
        case 1:
          // PUBACK
          // +------------+-----------------+-------+-----
          // | 001 --- TT | topic-len-bytes | topic |
          // +------------+-----------------+-------+-----
          final int topicLen = (int) ByteBufIntUtil.readFixed(packet, 1 + (head & 3));
          final ByteBuf topic = packet.readSlice(topicLen);
          final LogState state = client.topics.get(topic);
          pubAckReceived(state, head & 31, packet);
          tryPublish(state);
          break;
      }
    }

    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) throws Exception {
      if (evt instanceof final LogState state) {
        tryPublish(state);
      } else {
        super.userEventTriggered(ctx, evt);
      }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
      Logger.error(cause, "uncaught exception: {}", ctx.channel().remoteAddress());
      ctx.close();
    }

    private void tryPublish(final LogState state) {
      if (state.waiting > 0) {
        final long since = System.nanoTime() - state.waiting;
        if (since > TimeUnit.MINUTES.toNanos(5)) {
          Logger.warn("WAITING FOR {} ACK since {}", state.topic, HumansUtil.humanTimeNanos(since));
        } else {
          Logger.debug("WAITING FOR {} ACK since {}", state.topic, HumansUtil.humanTimeNanos(since));
        }
      } else {
        state.waiting = client.tryPublish(state);
      }
    }

    private void pubAckReceived(final LogState state, final int flags, final ByteBuf data) throws Exception {
      Logger.debug("ACK PACKET RECEIVED flags:{}", flags);
      switch (flags >> 2) {
        case 0:
          // PUBACK
          // +------------+-----------------+-------+
          // | 001 000 TT | topic-len-bytes | topic |
          // +------------+-----------------+-------+
          Logger.debug("ACK RECEIVED");
          state.consumeLastChunk(client.offsetStores);
          break;
        case 1:
          // PUBNAK RESET
          // +------------+-----------------+-------+---------------+
          // | 001 001 TT | topic-len-bytes | topic | reset-voffset |
          // +------------+-----------------+-------+---------------+
          final long offset = ByteBufIntUtil.readVarLong(data);
          Logger.warn("NACK/RESET RECEIVED offset:{}", offset);
          state.setOffset(client.offsetStores, offset);
          state.setLastSentChunkSize(0);
          break;
        case 2:
          // PUBNAK Failure
          // +------------+-----------------+-------+
          // | 001 010 TT | topic-len-bytes | topic |
          // +------------+-----------------+-------+
          Logger.warn("NACK RECEIVED");
          throw new UnsupportedOperationException();
      }
      state.waiting = 0;
    }
  }

  public static final class LogSyncClientStats extends TelemetryCollectorGroup {
    private final ConcurrentTimeRangeCounter sentSize = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_SIZE)
      .setName("sent_size")
      .setLabel("Sent Size")
      .register(this, new ConcurrentTimeRangeCounter(24 * 60, 1, TimeUnit.MINUTES));

    private final ConcurrentTimeRangeCounter availSize = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_SIZE)
      .setName("avail_size")
      .setLabel("Avail Size")
      .register(this, new ConcurrentTimeRangeCounter(24 * 60, 1, TimeUnit.MINUTES));

    private LogSyncClientStats() {
      // no-op
    }

    public static LogSyncClientStats getInstance(final String logId) {
      final String groupName = "log_sync_client_" + logId;

      final LogSyncClientStats stats = TelemetryCollectorRegistry.INSTANCE.get(groupName);
      if (stats != null) return stats;

      return TelemetryCollectorRegistry.INSTANCE.register(groupName, "Log Sync Client " + logId, null, new LogSyncClientStats());
    }

    public void send(final long offset, final int length, final long maxOffset) {
      final long now = TimeUtil.currentUtcMillis();
      sentSize.add(now, length);
      availSize.update(now, maxOffset - offset);
    }
  }
}
