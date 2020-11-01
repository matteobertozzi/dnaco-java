/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional infor
 * tion regarding copyright ownership.
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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import tech.dnaco.logging.Logger;
import tech.dnaco.logging.LoggerSession;
import tech.dnaco.server.ClientEventLoop;
import tech.dnaco.server.ServiceEventLoop;

public class BinaryClient {
  private final AtomicLong seqId = new AtomicLong(0);

  private final AtomicBoolean connected = new AtomicBoolean(false);
  private final Bootstrap bootstrap;
  private SocketAddress address;
  private ChannelFuture channel;

  public BinaryClient(final ServiceEventLoop eventLoop) {
    this(eventLoop.getWorkerGroup(), eventLoop.getClientChannelClass());
  }

  public BinaryClient(final ClientEventLoop eventLoop) {
    this(eventLoop.getWorkerGroup(), eventLoop.getChannelClass());
  }

  private BinaryClient(final EventLoopGroup workerGroup, final Class<? extends Channel> channelClass) {
    this.bootstrap = new Bootstrap()
      .group(workerGroup)
      .channel(channelClass)
      .option(ChannelOption.TCP_NODELAY, true)
      .handler(new ChannelInitializer<SocketChannel>() {
        @Override
        public void initChannel(final SocketChannel channel) throws Exception {
          final ChannelPipeline pipeline = channel.pipeline();
          pipeline.addLast(ZlibCodecFactory.newZlibEncoder(ZlibWrapper.GZIP));
          pipeline.addLast(ZlibCodecFactory.newZlibDecoder(ZlibWrapper.GZIP));
          pipeline.addLast(new LoggingHandler(LogLevel.TRACE));
          pipeline.addLast(BinaryEncoder.INSTANCE);
          pipeline.addLast(new BinaryDecoder());
          pipeline.addLast(new BinaryClientHandler(BinaryClient.this));
        }
      });
  }

  public BinaryClient connect(final String host, final int port) throws InterruptedException {
    return connect(InetSocketAddress.createUnresolved(host, port));
  }

  public BinaryClient connect(final SocketAddress address) throws InterruptedException {
    if (connected.get()) disconnect();

    this.address = address;
    this.channel = this.bootstrap.connect(address).sync();
    this.connected.set(true);
    return this;
  }

  public void disconnect() throws InterruptedException {
    if (!connected.get()) return;

    final ChannelFuture future = channel.channel().close();
    this.channel = null;
    future.sync();
  }

  private final ConcurrentHashMap<Long, BinaryPacketReceiver> subscriptionPackets = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Long, InFlightPacket> pendingPackets = new ConcurrentHashMap<>();

  public CompletableFuture<BinaryPacket> send(final int command, final byte[] data) {
    return send(command, Unpooled.wrappedBuffer(data));
  }

  public CompletableFuture<BinaryPacket> send(final int command, final ByteBuf data) {
    final long pkgId = seqId.incrementAndGet();
    final InFlightPacket future = new InFlightPacket(pkgId);
    future.whenComplete((result, exception) -> pendingPackets.remove(pkgId));
    pendingPackets.put(pkgId, future);
    channel.channel().writeAndFlush(BinaryPacket.alloc(pkgId, command, data));
    return future;
  }

  public void subscribe(final long pkgId, final BinaryPacketReceiver receiver) {
    subscriptionPackets.put(pkgId, receiver);
  }

  public void unsubscribe(final long pkgId) {
    subscriptionPackets.remove(pkgId);
  }

  private void reconnect() throws InterruptedException {
    if (!connected.get() || this.channel == null) {
      //System.out.println("TRYING TO RECONNECT a DISCONNECTED client");
      return;
    }

    this.channel.channel().close().sync();
    //this.seqId.set(0);
    this.channel = this.bootstrap.connect(address);
    //System.out.println("RECONNECT");
    try {
      this.channel.sync();
      //System.out.println(" ---> RECONNECT DONE");
    } catch (final Exception e) {
      //System.out.println(" ---> RECONNECT FAILED: " + e.getMessage());
    }
  }

  private final class InFlightPacket extends CompletableFuture<BinaryPacket> {
    private final long pkgId;

    private InFlightPacket(final long pkgId) {
      this.pkgId = pkgId;
    }

    private void received(final BinaryPacket respPacket) {
      this.complete(respPacket);
    }
  }

  private void packetReceived(final BinaryPacket packet) {
    // TODO: add a flag to indicate response vs push

    //System.out.println(" ---> PACKET RECEIVED " + pendingPackets.size());
    final InFlightPacket inFlightPacket = pendingPackets.remove(packet.getPkgId());
    if (inFlightPacket != null) {
      inFlightPacket.received(packet);
      return;
    }

    final BinaryPacketReceiver subReceiver = subscriptionPackets.get(packet.getPkgId());
    if (subReceiver != null) {
      subReceiver.packetReceived(packet);
      return;
    }

    Logger.warn("no receiver for packet: {}", packet);
  }

  public interface BinaryPacketReceiver {
    void packetReceived(final BinaryPacket packet);
  }

  private static final class BinaryClientHandler extends SimpleChannelInboundHandler<BinaryPacket> {
    private final BinaryClient client;
    private long lastAck = -1;

    private BinaryClientHandler(final BinaryClient client) {
      this.client = client;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final BinaryPacket msg) throws Exception {
      //System.out.println("CLIENT RECEIVED: " + msg);
      lastAck = Math.max(lastAck, msg.getPkgId());
      client.packetReceived(msg);
    }

    @Override
    public void channelUnregistered(final ChannelHandlerContext ctx) throws Exception {
      //System.out.println("UNREGISTERED");
      client.reconnect();
      ctx.fireChannelUnregistered();
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
      client.connected.set(true);
      System.out.println("ACTIVE");
      ctx.fireChannelInactive();
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
      System.out.println("INACTIVE");
      ctx.fireChannelInactive();
    }

    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) throws Exception {
      System.out.println("EVENT " + evt);
      ctx.fireUserEventTriggered(evt);
    }

    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause)
            throws Exception {
      Logger.setSession(LoggerSession.newSystemGeneralSession());
      Logger.error(cause, "uncaught exception");
      ctx.fireExceptionCaught(cause);
    }
  }
}
