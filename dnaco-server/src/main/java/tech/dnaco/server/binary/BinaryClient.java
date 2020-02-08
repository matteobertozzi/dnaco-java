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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import tech.dnaco.server.ServiceEventLoop;

public class BinaryClient {
  private final AtomicLong seqId = new AtomicLong(0);

  private final Bootstrap bootstrap;
  private SocketAddress address;
  private ChannelFuture channel;

  public BinaryClient(final ServiceEventLoop eventLoop) {
    final BinaryClientHandler handler = new BinaryClientHandler(this);
    this.bootstrap = new Bootstrap()
      .group(eventLoop.getWorkerGroup())
      .channel(eventLoop.getClientChannelClass())
      .option(ChannelOption.TCP_NODELAY, true)
      .handler(new ChannelInitializer<SocketChannel>() {
          @Override
          public void initChannel(SocketChannel ch) throws Exception {
              ch.pipeline().addLast(new LoggingHandler(LogLevel.TRACE));
              ch.pipeline().addLast(BinaryEncoder.INSTANCE);
              ch.pipeline().addLast(new BinaryDecoder());
              ch.pipeline().addLast(handler);
          }
      });
  }

  public void connect(final String host, final int port) throws InterruptedException {
    connect(InetSocketAddress.createUnresolved(host, port));
  }

  public void connect(final SocketAddress address) throws InterruptedException {
    this.address = address;
    this.channel = this.bootstrap.connect(address).sync();
  }

  public void disconnect() throws InterruptedException {
    this.channel.channel().closeFuture().sync();
  }

  public void send(final int command, final byte[] data, BinaryPacketReceiver receiver) {
    writePacket(new BinaryPacket(seqId.incrementAndGet(), command, data), receiver);
  }

  public byte[] send(final int command, final byte[] data, final int timeout, final TimeUnit unit) throws InterruptedException {
    final AtomicReference<byte[]> result = new AtomicReference<>();
    final CountDownLatch latch = new CountDownLatch(1);
    send(command, data, (resp) -> {
      result.set(resp.getData());
      latch.countDown();
    });
    latch.await(timeout, unit);
    return result.get();
  }

  private void reconnect() throws InterruptedException {
    this.channel.channel().close();
    this.seqId.set(0);
    this.channel = this.bootstrap.connect(address);
    System.out.println("RECONNECT");
  }

  private final HashMap<BinaryPacket, BinaryPacketReceiver> pendingPackets = new HashMap<>();
  private void writePacket(final BinaryPacket packet, final BinaryPacketReceiver receiver) {
    pendingPackets.put(packet, receiver);
    channel.channel().writeAndFlush(packet);
  }

  private void packetReceived(final BinaryPacket packet) {
    System.out.println(" ---> PACKET RECEIVED " + pendingPackets.size());
    final Iterator<Entry<BinaryPacket, BinaryPacketReceiver>> it = pendingPackets.entrySet().iterator();
    while (it.hasNext()) {
      final Entry<BinaryPacket, BinaryPacketReceiver> entry = it.next();
      if (entry.getKey().getPkgId() == packet.getPkgId()) {
        entry.getValue().packetReceived(packet);
        it.remove();
        break;
      }
    }
  }

  public interface BinaryPacketReceiver {
    void packetReceived(final BinaryPacket packet);
  }

  @Sharable
  private static final class BinaryClientHandler extends SimpleChannelInboundHandler<BinaryPacket> {
    private final BinaryClient client;
    private long lastAck = -1;

    private BinaryClientHandler(final BinaryClient client) {
      this.client = client;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, BinaryPacket msg) throws Exception {
      System.out.println("CLIENT RECEIVED: " + msg);
      lastAck = Math.max(lastAck, msg.getPkgId());
      client.packetReceived(msg);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
      client.reconnect();
      System.out.println("UNREGISTERED");
        ctx.fireChannelUnregistered();
    }
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      System.out.println("INACTIVE");
        ctx.fireChannelInactive();
    }
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      System.out.println("EVENT " + evt);
        ctx.fireUserEventTriggered(evt);
    }
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
              System.out.println("EXCEPTION " + cause.getMessage());
        ctx.fireExceptionCaught(cause);
    }
  }
}
