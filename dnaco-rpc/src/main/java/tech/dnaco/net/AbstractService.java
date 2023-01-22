/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package tech.dnaco.net;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FileRegion;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.channel.unix.DomainSocketChannel;
import io.netty.channel.unix.UnixChannel;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutor;
import tech.dnaco.logging.Logger;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.telemetry.ConcurrentTimeRangeCounter;
import tech.dnaco.telemetry.ConcurrentTimeRangeGauge;
import tech.dnaco.telemetry.TelemetryCollector;
import tech.dnaco.threading.ShutdownUtil;
import tech.dnaco.tracing.Span;
import tech.dnaco.tracing.TraceAttributes;
import tech.dnaco.tracing.Tracer;

public abstract class AbstractService implements ShutdownUtil.StopSignal {
  private final CopyOnWriteArrayList<Channel> channels = new CopyOnWriteArrayList<>();
  private final AtomicBoolean running = new AtomicBoolean(false);

  protected AbstractService() {
    // no-op
  }

  protected AtomicBoolean running() {
    return running;
  }

  protected boolean isRunning() {
    return running.get();
  }

  protected abstract void setupPipeline(ChannelPipeline pipeline);

  protected void setupTcpServerBootstrap(final ServerBootstrap bootstrap) {
    bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(final SocketChannel channel) {
        setupPipeline(channel.pipeline());
      }
    });
  }

  protected void setupUnixServerBootstrap(final ServerBootstrap bootstrap) {
    bootstrap.childHandler(new ChannelInitializer<UnixChannel>() {
      @Override
      public void initChannel(final UnixChannel channel) {
        setupPipeline(channel.pipeline());
      }
    });
  }

  protected static ServerBootstrap newTcpServerBootstrap(final ServiceEventLoop eventLoop, final EventLoopGroup workerGroup) {
    return new ServerBootstrap()
      .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
      .option(ChannelOption.SO_BACKLOG, 1024)
      .option(ChannelOption.SO_REUSEADDR, true)
      .childOption(ChannelOption.TCP_NODELAY, true)
      .group(eventLoop.getBossGroup(), workerGroup)
      .channel(eventLoop.getServerChannelClass());
  }

  protected static ServerBootstrap newUnixServerBootstrap(final ServiceEventLoop eventLoop, final EventLoopGroup workerGroup) {
    return new ServerBootstrap()
      .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
      .option(ChannelOption.SO_BACKLOG, 1024)
      .option(ChannelOption.SO_REUSEADDR, true)
      .group(eventLoop.getBossGroup(), workerGroup)
      .channel(eventLoop.getServerUnixChannelClass());
  }

  // ================================================================================
  //  Bind TCP Service
  // ================================================================================
  public void bindTcpService(final ServiceEventLoop eventLoop, final int port) throws InterruptedException {
    bindTcpService(eventLoop, eventLoop.getWorkerGroup(), port);
  }

  public void bindTcpService(final ServiceEventLoop eventLoop, final EventLoopGroup workerGroup, final int port) throws InterruptedException {
    bindTcpService(eventLoop, workerGroup, new InetSocketAddress(port));
  }

  public void bindTcpService(final ServiceEventLoop eventLoop, final InetSocketAddress address) throws InterruptedException {
    bindTcpService(eventLoop, eventLoop.getWorkerGroup(), address);
  }

  public void bindTcpService(final ServiceEventLoop eventLoop, final EventLoopGroup workerGroup,
      final InetSocketAddress address) throws InterruptedException {
    final ServerBootstrap bootstrap = newTcpServerBootstrap(eventLoop, workerGroup);
    setupTcpServerBootstrap(bootstrap);
    bind(bootstrap, address);
  }

  // ================================================================================
  //  Bind Unix Service
  // ================================================================================
  public void bindUnixService(final ServiceEventLoop eventLoop, final File sock) throws InterruptedException {
    bindUnixService(eventLoop, eventLoop.getWorkerGroup(), sock);
  }

  public void bindUnixService(final ServiceEventLoop eventLoop, final EventLoopGroup workerGroup, final File sock) throws InterruptedException {
    bindUnixService(eventLoop, workerGroup, new DomainSocketAddress(sock));
  }

  public void bindUnixService(final ServiceEventLoop eventLoop, final DomainSocketAddress address) throws InterruptedException {
    bindUnixService(eventLoop, eventLoop.getWorkerGroup(), address);
  }

  public void bindUnixService(final ServiceEventLoop eventLoop, final EventLoopGroup workerGroup,
      final DomainSocketAddress address) throws InterruptedException {
    final ServerBootstrap bootstrap = newUnixServerBootstrap(eventLoop, workerGroup);
    setupUnixServerBootstrap(bootstrap);
    bind(bootstrap, address);
  }

  // ================================================================================
  //  Bind helpers
  // ================================================================================
  private void bind(final ServerBootstrap bootstrap, final SocketAddress address) throws InterruptedException {
    final Channel channel = bootstrap.bind(address).sync().channel();
    Logger.info("{} service listening on {}", getClass().getSimpleName(), address);
    this.running.set(true);
    this.channels.add(channel);
  }

  // ================================================================================
  //  Shutdown helpers
  // ================================================================================
  @Override
  public boolean sendStopSignal() {
    running.set(false);
    for (final Channel channel: this.channels) {
      channel.close();
    }
    return true;
  }

  public void waitStopSignal() throws InterruptedException {
    for (final Channel channel: this.channels) {
      channel.closeFuture().sync();
    }
    running.set(false);
    channels.clear();
  }

  public void addShutdownHook() {
    ShutdownUtil.addShutdownHook(toString(), this);
  }

  // ================================================================================
  //  Network In/Out Stats
  // ================================================================================
  protected void addNetworkIoStats(final ChannelPipeline pipeline) {
    pipeline.addLast(InboundHandlerStats.INSTANCE);
    pipeline.addLast(OutboundHandlerStats.INSTANCE);
  }

  @Sharable
  private static final class OutboundHandlerStats extends ChannelOutboundHandlerAdapter {
    private static final OutboundHandlerStats INSTANCE = new OutboundHandlerStats();

    private final ConcurrentTimeRangeCounter netOutBytes = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_SIZE)
      .setName("service_net_out_bytes")
      .setLabel("Service Network Out Bytes")
      .register(new ConcurrentTimeRangeCounter(24 * 60, 1, TimeUnit.MINUTES));

    private OutboundHandlerStats() {
      // no-op
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) throws Exception {
      computeWriteStats(msg);
      super.write(ctx, msg, promise);
    }

    private void computeWriteStats(final Object msg) {
      if (msg instanceof final ByteBuf byteBuf) {
        netOutBytes.inc(byteBuf.readableBytes());
      } else if (msg instanceof final FileRegion fileRegion) {
        netOutBytes.inc(fileRegion.count());
      } else {
        Logger.warn("unhandled WRITE type:{} -> {}", msg.getClass(), msg);
      }
    }
  }

  @Sharable
  private static final class InboundHandlerStats extends ChannelInboundHandlerAdapter {
    private static final InboundHandlerStats INSTANCE = new InboundHandlerStats();

    private final ConcurrentTimeRangeCounter netInBytes = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_SIZE)
      .setName("service_net_in_bytes")
      .setLabel("Service Network In Bytes")
      .register(new ConcurrentTimeRangeCounter(24 * 60, 1, TimeUnit.MINUTES));

    private InboundHandlerStats() {
      // no-op
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
      computeReadStats(msg);
      super.channelRead(ctx, msg);
    }

    private void computeReadStats(final Object msg) {
      if (msg instanceof final ByteBuf byteBuf) {
        netInBytes.inc(byteBuf.readableBytes());
      } else {
        Logger.warn("unhandled READ type:{} -> {}", msg.getClass(), msg);
      }
    }
  }

  // ================================================================================
  //  Client connection related
  // ================================================================================
  public static abstract class AbstractServiceSession {
    private final ChannelHandlerContext ctx;

    protected AbstractServiceSession(final ChannelHandlerContext ctx) {
      this.ctx = ctx;
    }

    public ChannelFuture close() {
      return ctx.close();
    }

    public ChannelFuture write(final Object msg) {
      return ctx.write(msg);
    }

    public ChannelFuture writeAndFlush(final Object msg) {
      return ctx.writeAndFlush(msg);
    }

    public void flush() {
      ctx.flush();
    }

    public SocketAddress remoteAddress() {
      return ctx.channel().remoteAddress();
    }

    public EventExecutor executor() {
      return ctx.executor();
    }

    public Channel getChannel() {
      return ctx.channel();
    }
  }

  private static final AttributeKey<AbstractServiceSession> SESSION_ATTR_KEY = AttributeKey.valueOf("sid");

  @SuppressWarnings("unchecked")
  public static <T extends AbstractServiceSession> T getSession(final Channel channel) {
    return (T) channel.attr(SESSION_ATTR_KEY).get();
  }

  protected static abstract class ServiceChannelInboundHandler<T> extends SimpleChannelInboundHandler<T> {
    private final AtomicLong activeChannels = new AtomicLong();

    private final ConcurrentTimeRangeGauge activeConnectionsGauge = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_COUNT)
      .setName("services_active_connections")
      .setLabel("Service Active Connections")
      .register(new ConcurrentTimeRangeGauge(24 * 60, 1, TimeUnit.MINUTES));

    private final ConcurrentTimeRangeCounter newConnections = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_COUNT)
      .setName("services_new_connections")
      .setLabel("Service New Connections")
      .register(new ConcurrentTimeRangeCounter(24 * 60, 1, TimeUnit.MINUTES));

    protected ServiceChannelInboundHandler() {
      super();
    }

    protected static <T extends AbstractServiceSession> T getSession(final Channel channel) {
      return AbstractService.getSession(channel);
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) {
      ctx.flush();
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
      super.channelActive(ctx);
      activeConnectionsGauge.inc();
      newConnections.inc();

      try (Span span = Tracer.newTask()) {
        span.setLabel("client connected");
        span.setAttribute(TraceAttributes.MODULE, getClass().getSimpleName());

        final long count = activeChannels.incrementAndGet();
        final Channel channel = ctx.channel();
        Logger.trace("channel active: {} {} {}", count, channelType(channel), channel.remoteAddress());

        try {
          final AbstractServiceSession session = sessionConnected(ctx);
          if (session != null) channel.attr(SESSION_ATTR_KEY).set(session);
        } catch (final Throwable e) {
          Logger.error(e, "failed to create session for {}, close the connection.", channel);
          ctx.close();
        }
      }
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
      try (Span span = Tracer.newTask()) {
        span.setLabel("client disconnected");
        span.setAttribute(TraceAttributes.MODULE, getClass().getSimpleName());

        final Channel channel = ctx.channel();
        final long count = activeChannels.decrementAndGet();
        Logger.trace("channel inactive: {} {} {}", count, channelType(channel), channel.remoteAddress());

        final AbstractServiceSession session = channel.attr(SESSION_ATTR_KEY).getAndSet(null);
        if (session != null) {
          try {
            sessionDisconnected(session);
          } catch (final Throwable e) {
            Logger.error(e, "failed to destroy the session for {}", channel);
          }
        }
      }
      activeConnectionsGauge.dec();
      super.channelInactive(ctx);
    }

    protected abstract AbstractServiceSession sessionConnected(final ChannelHandlerContext ctx);
    protected abstract void sessionDisconnected(final AbstractServiceSession session);

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
      Logger.error(cause, "uncaught exception: {}", ctx.channel().remoteAddress());
      ctx.close();
    }
  }

  private static String channelType(final Channel channel) {
    if (channel instanceof DomainSocketChannel) return "unix";
    if (channel instanceof SocketChannel) return "tcp";
    return "unknown";
  }
}
