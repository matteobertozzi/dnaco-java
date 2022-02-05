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
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.channel.unix.DomainSocketChannel;
import io.netty.channel.unix.UnixChannel;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutor;
import tech.dnaco.logging.Logger;
import tech.dnaco.threading.ShutdownUtil;
import tech.dnaco.tracing.Span;
import tech.dnaco.tracing.TraceAttributes;
import tech.dnaco.tracing.Tracer;

public abstract class AbstractService implements ShutdownUtil.StopSignal {
  private final ArrayList<Channel> channels = new ArrayList<>();
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

      try (Span span = Tracer.newTask()) {
        span.setLabel("client connected");
        span.setAttribute(TraceAttributes.MODULE, getClass().getSimpleName());

        final long count = activeChannels.incrementAndGet();
        final Channel channel = ctx.channel();
        Logger.debug("channel active: {} {} {}", count, channelType(channel), channel.remoteAddress());

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
        Logger.debug("channel inactive: {} {} {}", count, channelType(channel), channel.remoteAddress());

        final AbstractServiceSession session = channel.attr(SESSION_ATTR_KEY).getAndSet(null);
        if (session != null) {
          try {
            sessionDisconnected(session);
          } catch (final Throwable e) {
            Logger.error(e, "failed to destroy the session for {}", channel);
          }
        }
      }
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
