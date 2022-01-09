/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.dnaco.net.frame;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import tech.dnaco.logging.Logger;
import tech.dnaco.net.AbstractClient;
import tech.dnaco.net.AbstractService;
import tech.dnaco.net.ServiceEventLoop;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.time.RetryUtil;
import tech.dnaco.time.RetryUtil.RetryLogic;

public class DemoFrameEcho {
  private static class EchoService extends AbstractService {

    @Override
    protected void setupPipeline(final ChannelPipeline pipeline) {
      pipeline.addLast(new DnacoFrameDecoder());
      pipeline.addLast(DnacoFrameEncoder.INSTANCE);
      pipeline.addLast(new EchoServiceHandler());
    }

    private static class EchoServiceHandler extends SimpleChannelInboundHandler<DnacoFrame> {

      @Override
      protected void channelRead0(final ChannelHandlerContext ctx, final DnacoFrame msg) throws Exception {
        ctx.writeAndFlush(msg.retain());
      }

    }
  }

  private static class EchoClient extends AbstractClient {
    protected EchoClient(final Bootstrap bootstrap, final RetryLogic retryLogic) {
      super(bootstrap, retryLogic);
    }

    public static EchoClient newTcpClient(final EventLoopGroup eloopGroup,
        final Class<? extends Channel> channelClass,
        final RetryUtil.RetryLogic retryLogic) {
      final Bootstrap bootstrap = newTcpClientBootstrap(eloopGroup, channelClass);

      final EchoClient client = new EchoClient(bootstrap, retryLogic);
      client.setupTcpServerBootstrap();
      return client;
    }

    @Override
    protected void setupPipeline(final ChannelPipeline pipeline) {
      pipeline.addLast(new DnacoFrameDecoder());
      pipeline.addLast(DnacoFrameEncoder.INSTANCE);
      pipeline.addLast(new EchoClientHandler());
    }
    private static final int NRUNS = 1_000_000;
    private long startTime;
    private int count = 0;
    private class EchoClientHandler extends SimpleChannelInboundHandler<DnacoFrame> {
      @Override
      protected void channelRead0(final ChannelHandlerContext ctx, final DnacoFrame msg) throws Exception {
        //System.out.println("client recv: " + msg.getData().toString(StandardCharsets.UTF_8));

        if (count >= NRUNS) {
          final long elapsed = System.nanoTime() - startTime;
          System.out.println(HumansUtil.humanRate(NRUNS / (elapsed / 1000000000.0))
            + " -> " + HumansUtil.humanTimeNanos(elapsed));
          ctx.close();
          return;
        }

        write(DnacoFrame.alloc(7, Unpooled.wrappedBuffer(("hello-" + count++).getBytes())));
      }

      @Override
      public void channelReadComplete(final ChannelHandlerContext ctx) {
        flush();
      }

      @Override
      public void channelActive(final ChannelHandlerContext ctx) {
        setState(ClientState.CONNECTED);
        Logger.debug("channel registered: {}", ctx.channel().remoteAddress());
        setReady();
        ctx.fireChannelActive();

        if (count == 0) {
          startTime = System.nanoTime();
          writeAndFlush(DnacoFrame.alloc(7, Unpooled.wrappedBuffer(("hello-" + count++).getBytes())));
        }
      }

      @Override
      public void channelInactive(final ChannelHandlerContext ctx) {
        setState(ClientState.DISCONNECTED);
        Logger.debug("channel unregistered: {}", ctx.channel().remoteAddress());
        ctx.fireChannelInactive();
      }

      @Override
      public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
        //Logger.setSession(LoggerSession.newSystemGeneralSession());
        Logger.error(cause, "uncaught exception: {}", ctx.channel().remoteAddress());
        ctx.close();
      }
    }
  }

  public static void main(final String[] args) throws Exception {
    try (ServiceEventLoop eloop = new ServiceEventLoop(1, 8)) {
      final EchoService service = new EchoService();
      service.bindTcpService(eloop, 8889);

      /*
      final EchoClient client = EchoClient.newTcpClient(eloop.getWorkerGroup(), eloop.getClientChannelClass(), RetryUtil.newFixedRetry(1000));
      client.connect("127.0.0.1", 8889);
      while (!client.isReady()) Thread.yield();
      ThreadUtil.sleep(1, TimeUnit.MINUTES);
      client.waitForShutdown();
      */

      service.addShutdownHook();
      service.waitStopSignal();
    }
  }
}
