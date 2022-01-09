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

package tech.dnaco.net.message;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.jctools.maps.NonBlockingHashMapLong;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import tech.dnaco.logging.Logger;
import tech.dnaco.net.AbstractClient;
import tech.dnaco.net.frame.DnacoFrameDecoder;
import tech.dnaco.net.frame.DnacoFrameEncoder;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.time.RetryUtil;
import tech.dnaco.time.RetryUtil.RetryLogic;

public class DnacoMessageClient extends AbstractClient {

  protected DnacoMessageClient(final Bootstrap bootstrap, final RetryLogic retryLogic) {
    super(bootstrap, retryLogic);
  }

  public static DnacoMessageClient newTcpClient(final EventLoopGroup eloopGroup,
      final Class<? extends Channel> channelClass,
      final RetryUtil.RetryLogic retryLogic) {
    final Bootstrap bootstrap = newTcpClientBootstrap(eloopGroup, channelClass);

    final DnacoMessageClient client = new DnacoMessageClient(bootstrap, retryLogic);
    client.setupTcpServerBootstrap();
    return client;
  }

  public static DnacoMessageClient newUnixClient(final EventLoopGroup eloopGroup,
      final Class<? extends Channel> channelClass,
      final RetryUtil.RetryLogic retryLogic) {
    final Bootstrap bootstrap = newUnixClientBootstrap(eloopGroup, channelClass);

    final DnacoMessageClient client = new DnacoMessageClient(bootstrap, retryLogic);
    client.setupUnixServerBootstrap();
    return client;
  }

  @Override
  protected void setupPipeline(final ChannelPipeline pipeline) {
    pipeline.addLast(new DnacoFrameDecoder());
    pipeline.addLast(DnacoMessageDecoder.INSTANCE);
    pipeline.addLast(DnacoFrameEncoder.INSTANCE);
    pipeline.addLast(DnacoMessageEncoder.INSTANCE);
    pipeline.addLast(new DnacoMessageClientHandler(this));
  }

  private Consumer<DnacoMessageClient> connectedHandler;
  public DnacoMessageClient whenConnected(final Consumer<DnacoMessageClient> consumer) {
    this.connectedHandler = consumer;
    return this;
  }

  private final AtomicLong pkgId = new AtomicLong();
  public ClientPromise<DnacoMessage> sendMessage(final ByteBuf data) {
    return sendMessage(new DnacoMessage(pkgId.incrementAndGet(), null, data));
  }

  public ClientPromise<DnacoMessage> sendMessage(final DnacoMetadataMap metadata, final ByteBuf data) {
    return sendMessage(new DnacoMessage(pkgId.incrementAndGet(), metadata, data));
  }

  public ClientPromise<DnacoMessage> sendMessage(final DnacoMessage request) {
    final ClientPromise<DnacoMessage> future = newFuture(request);
    writeAndFlush(request);
    return future;
  }

  private final NonBlockingHashMapLong<InProgressClientPromise<DnacoMessage>> responsesFutures = new NonBlockingHashMapLong<>();
  private ClientPromise<DnacoMessage> newFuture(final DnacoMessage request) {
    final InProgressClientPromise<DnacoMessage> future = new InProgressClientPromise<>();
    responsesFutures.put(request.packetId(), future);
    return future;
  }

  private static final class DnacoMessageClientHandler extends SimpleChannelInboundHandler<DnacoMessage> {
    private final DnacoMessageClient client;

    public DnacoMessageClientHandler(final DnacoMessageClient client) {
      this.client = client;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final DnacoMessage msg) throws Exception {
      final long startNs = System.nanoTime();
      final InProgressClientPromise<DnacoMessage> future = client.responsesFutures.remove(msg.packetId());
      if (future != null) {
        future.setSuccess(msg);
        final long elapsedNs = System.nanoTime() - startNs;
        Logger.trace("response {} handled in {}", msg, HumansUtil.humanTimeNanos(elapsedNs));
      } else {
        Logger.warn("no response future waiting for response: {}", msg);
      }
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) {
      ctx.flush();
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) {
      client.setState(ClientState.CONNECTED);
      Logger.debug("channel registered: {}", ctx.channel().remoteAddress());
      if (client.connectedHandler != null) {
        client.connectedHandler.accept(client);
      } else {
        client.setReady();
      }
      ctx.fireChannelActive();
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) {
      client.setState(ClientState.DISCONNECTED);
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
