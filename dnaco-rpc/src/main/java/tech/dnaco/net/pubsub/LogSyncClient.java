package tech.dnaco.net.pubsub;

import java.io.File;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
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
import tech.dnaco.net.pubsub.LogFileUtil.LogOffsetStore;
import tech.dnaco.net.pubsub.LogFileUtil.LogsTracker;
import tech.dnaco.net.util.ByteBufIntUtil;
import tech.dnaco.time.RetryUtil;

public class LogSyncClient extends AbstractClient {
  public static final class LogState {
    private final LogsTracker tracker;
    private final ByteBuf topic;

    private boolean waiting = false;
    private int lastSentChunkSize;

    public LogState(final LogsTracker consumer, final ByteBuf topic, final int lastSentChunkSize) {
      this.tracker = consumer;
      this.topic = topic;
      this.lastSentChunkSize = lastSentChunkSize;
    }

    public void setLastSentChunkSize(final int size) {
      this.lastSentChunkSize = size;
    }

    public LogsTracker consumer() {
      return this.tracker;
    }

    public ByteBuf topic() {
      return this.topic;
    }

    public void consumeLastChunk(final List<LogOffsetStore> stores) throws Exception {
      final long newOffset = tracker.getOffset() + lastSentChunkSize;
      for (final LogOffsetStore store: stores) {
        store.store(tracker, newOffset);
      }
      tracker.consume(lastSentChunkSize);
    }

    public void setOffset(final List<LogOffsetStore> stores, final long offset) throws Exception {
      for (final LogOffsetStore store: stores) {
        store.store(tracker, offset);
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

  public void add(final LogsTracker tracker) {
    final ByteBuf topic = Unpooled.wrappedBuffer(tracker.getName().getBytes());
    final LogState state = new LogState(tracker, topic, 0);
    topics.put(topic, state);
    trackers.add(state);
    tracker.registerDataPublishedListener(x -> this.dataPublished(state));
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

  private boolean tryPublish(final LogState state) {
    final LogsTracker consumer = state.consumer();
    if (!consumer.hasMore()) {
      Logger.debug("NOTHING TO PUBLISH");
      return false;
    }

    final FileRegion fileRegion = getFileRegion(consumer);
    final long offset = consumer.getOffset();
    final int length = (int) fileRegion.count();
    state.setLastSentChunkSize(length);
    Logger.trace("PUBLISH: SEND OFFSET:{} LENGTH:{}", offset, length);

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
    return true;
  }

  private DefaultFileRegion getFileRegion(final LogsTracker consumer) {
    final File logFile = consumer.getBlockFile();
    final long logOffset = consumer.getBlockOffset();
    final long logAvail = consumer.getBlockAvailable();
    final long count = Math.min(logAvail, 1 << 20);
    Logger.debug("TRY PUBLISH REGION {} logOff={} logAvail={} count={} length={}",
      logFile, logOffset, logAvail, count, logFile.length());
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

    private void tryPublish(final LogState state) {
      if (state.waiting) {
        Logger.debug("WAITING FOR ACK");
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
      state.waiting = false;
    }
  }
}
