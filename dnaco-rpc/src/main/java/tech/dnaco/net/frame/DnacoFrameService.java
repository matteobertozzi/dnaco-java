package tech.dnaco.net.frame;

import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import tech.dnaco.net.AbstractService;

public class DnacoFrameService extends AbstractService {
  private final DnacoFrameHandler handler;

  public DnacoFrameService(final DnacoFrameServiceProcessor processor) {
    this.handler = new DnacoFrameHandler(processor, running());
  }

  @Override
  protected void setupPipeline(final ChannelPipeline pipeline) {
    pipeline.addLast(new DnacoFrameDecoder());
    pipeline.addLast(DnacoFrameEncoder.INSTANCE);
    pipeline.addLast(handler);
  }

  @Sharable
  private static class DnacoFrameHandler extends ServiceChannelInboundHandler<DnacoFrame> {
    private final DnacoFrameServiceProcessor processor;
    private final AtomicBoolean running;

    private DnacoFrameHandler(final DnacoFrameServiceProcessor processor, final AtomicBoolean running) {
      this.processor = processor;
      this.running = running;
    }

    @Override
    protected AbstractServiceSession sessionConnected(final ChannelHandlerContext ctx) {
      return processor.sessionConnected(ctx);
    }

    @Override
    protected void sessionDisconnected(final AbstractServiceSession session) {
      processor.sessionDisconnected(session);
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final DnacoFrame msg) throws Exception {
      if (running.get()) {
        processor.sessionMessageReceived(ctx, msg);
      } else {
        ctx.close();
      }
    }
  }

  public interface DnacoFrameServiceProcessor {
    default AbstractServiceSession sessionConnected(final ChannelHandlerContext ctx) { return null; }
    default void sessionDisconnected(final AbstractServiceSession session) {}

    void sessionMessageReceived(ChannelHandlerContext ctx, DnacoFrame message) throws Exception;
  }
}
