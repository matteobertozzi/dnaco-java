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

package tech.dnaco.server.binary;

import java.util.concurrent.CopyOnWriteArrayList;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.util.AttributeKey;
import tech.dnaco.logging.Logger;
import tech.dnaco.logging.LoggerSession;
import tech.dnaco.server.AbstractService;
import tech.dnaco.server.ServiceEventLoop;

public class BinaryService extends AbstractService {
  private static final AttributeKey<BinaryServiceSession> ATTR_KEY_SESSION = AttributeKey.valueOf("sid");

  private final CopyOnWriteArrayList<BinaryServiceListener> listeners = new CopyOnWriteArrayList<>();

  public BinaryService(final ServiceEventLoop eventLoop) {
    setBootstrap(newTcpServerBootstrap(eventLoop, new ChannelInitializer<SocketChannel>() {
      private final BinaryServiceHandler handler = new BinaryServiceHandler(BinaryService.this);

      @Override
      public void initChannel(final SocketChannel channel) throws Exception {
        final ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast(ZlibCodecFactory.newZlibEncoder(ZlibWrapper.GZIP));
        pipeline.addLast(ZlibCodecFactory.newZlibDecoder(ZlibWrapper.GZIP));
        pipeline.addLast(BinaryEncoder.INSTANCE);
        pipeline.addLast(new BinaryDecoder());
        pipeline.addLast(handler);
      }
    }));
  }

  public void registerListener(final BinaryServiceListener listener) {
    this.listeners.add(listener);
  }

  public void unregisterListener(final BinaryServiceListener listener) {
    this.listeners.remove(listener);
  }

  public interface BinaryServiceListener {
    void connect(BinaryServiceSession session);
    void disconnect(BinaryServiceSession session);
    void packetReceived(BinaryServiceSession session, BinaryPacket packet);
  }

  @Sharable
  private static final class BinaryServiceHandler extends SimpleChannelInboundHandler<BinaryPacket> {
    private final BinaryService service;

    private BinaryServiceHandler(final BinaryService service) {
      this.service = service;
    }

    @Override
    public void channelRead0(final ChannelHandlerContext ctx, final BinaryPacket packet) {
      final BinaryServiceSession session = ctx.channel().attr(ATTR_KEY_SESSION).get();
      for (final BinaryServiceListener listener: service.listeners) {
        listener.packetReceived(session, packet);
      }
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) {
      ctx.flush();
    }

    @Override
    public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
      Logger.debug("channel registered: {}", ctx.channel().remoteAddress());

      final BinaryServiceSession session = new BinaryServiceSession(ctx);
      ctx.channel().attr(ATTR_KEY_SESSION).set(session);
      for (final BinaryServiceListener listener: service.listeners) {
        listener.connect(session);
      }

      ctx.fireChannelRegistered();
    }

    @Override
    public void channelUnregistered(final ChannelHandlerContext ctx) throws Exception {
      Logger.debug("channel unregistered: {}", ctx.channel().remoteAddress());

      final BinaryServiceSession session = ctx.channel().attr(ATTR_KEY_SESSION).getAndSet(null);
      for (final BinaryServiceListener listener: service.listeners) {
        listener.disconnect(session);
      }

      ctx.fireChannelUnregistered();
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
      Logger.setSession(LoggerSession.newSystemGeneralSession());
      Logger.error(cause, "uncaught exception");
      ctx.close();
    }
  }
}