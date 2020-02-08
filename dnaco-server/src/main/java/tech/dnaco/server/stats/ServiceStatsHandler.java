package tech.dnaco.server.stats;

import java.util.concurrent.TimeUnit;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.AttributeKey;
import tech.dnaco.logging.Logger;
import tech.dnaco.strings.HumansUtil;

@Sharable
public class ServiceStatsHandler extends ChannelDuplexHandler {
  private static final AttributeKey<ChannelStats> ATTR_KEY_STATS = AttributeKey.valueOf("stats");

  private ServiceStats stats;

  public ServiceStatsHandler(final ServiceStats stats) {
    this.stats = stats;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    ctx.channel().attr(ATTR_KEY_STATS).set(new ChannelStats());
    stats.addChannel(ctx.channel().remoteAddress());
    ctx.executor().schedule(new SilentChannelKiller(ctx), 5, TimeUnit.SECONDS);
    Logger.debug("active");
    super.channelActive(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    final ChannelStats channelStats = ctx.channel().attr(ATTR_KEY_STATS).getAndSet(null);
    stats.removeChannel();

    Logger.debug("inactive: {}", HumansUtil.humanTimeSince(channelStats.connectionTime));
    super.channelInactive(ctx);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    final ChannelStats channelStats = getChannelStats(ctx);
    if (channelStats.firstReadTime == 0) {
      channelStats.firstReadTime = System.nanoTime();
      channelStats.lastReadTime = channelStats.firstReadTime;
    } else {
      channelStats.lastReadTime = System.nanoTime();
    }

    Logger.debug("read");
    super.channelRead(ctx, msg);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    getChannelStats(ctx).lastWriteTime = System.nanoTime();

    Logger.debug("write");
    super.write(ctx, msg, promise);
  }

  public static ChannelStats getChannelStats(final ChannelHandlerContext ctx) {
    return ctx.channel().attr(ATTR_KEY_STATS).get();
  }

  private static final class SilentChannelKiller implements Runnable {
    private final ChannelHandlerContext ctx;

    public SilentChannelKiller(ChannelHandlerContext ctx) {
      this.ctx = ctx;
	  }

	  @Override
    public void run() {
      final ChannelStats stats = getChannelStats(ctx);
      if (stats != null && stats.lastReadTime == 0) {
        Logger.warn("channel was silent for {}. killing it!", HumansUtil.humanTimeSince(stats.connectionTime));
        ctx.close();
      }
    }
  }

  public static final class ChannelStats {
    private final long connectionTime;
    private long firstReadTime;
    private long lastReadTime;
    private long lastWriteTime;

    private ChannelStats() {
      this.connectionTime = System.nanoTime();
    }

    public long markReadAsComplete() {
      final long elapsed = System.nanoTime() - firstReadTime;
      firstReadTime = 0;
      return elapsed;
    }
  }
}