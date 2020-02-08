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

package tech.dnaco.server;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.concurrent.UnorderedThreadPoolEventExecutor;
import tech.dnaco.logging.Logger;
import tech.dnaco.util.NamedThreadFactory;

public class ServiceEventLoop implements AutoCloseable {
  private final Class<? extends ServerChannel> serverChannelClass;
  private final Class<? extends SocketChannel> clientChannelClass;
  private final EventExecutorGroup scheduledGroup;
  private final EventLoopGroup workerGroup;
  private final EventLoopGroup bossGroup;

  public ServiceEventLoop(final int bossGroups, final int workerGroups) {
    this(true, bossGroups, workerGroups);
  }

  public ServiceEventLoop(final boolean useNative, final int bossGroups, final int workerGroups) {
    this(useNative, bossGroups, workerGroups, 2);
  }

  public ServiceEventLoop(final boolean useNative, final int bossGroups, final int workerGroups, final int scheduledGroups) {
    if (useNative && Epoll.isAvailable()) {
      bossGroup = new EpollEventLoopGroup(bossGroups, new NamedThreadFactory("epollServerBossGroup"));
      workerGroup = new EpollEventLoopGroup(workerGroups, new NamedThreadFactory("epollServerWorkerGroup"));
      serverChannelClass = EpollServerSocketChannel.class;
      clientChannelClass = EpollSocketChannel.class;
      Logger.info("Using epoll event loop - bossGroup={} workerGroup={}", bossGroups, workerGroups);
    } else if (useNative && KQueue.isAvailable()) {
      bossGroup = new KQueueEventLoopGroup(bossGroups, new NamedThreadFactory("kqueueServerBossGroup"));
      workerGroup = new KQueueEventLoopGroup(workerGroups, new NamedThreadFactory("kqueueServerWorkerGroup"));
      serverChannelClass = KQueueServerSocketChannel.class;
      clientChannelClass = KQueueSocketChannel.class;
      Logger.info("Using kqueue event loop - bossGroup={} workerGroup={}", bossGroups, workerGroups);
    } else {
      bossGroup = new NioEventLoopGroup(bossGroups, new NamedThreadFactory("nioServerBossGroup"));
      workerGroup = new NioEventLoopGroup(workerGroups, new NamedThreadFactory("nioServerWorkerGroup"));
      serverChannelClass = NioServerSocketChannel.class;
      clientChannelClass = NioSocketChannel.class;
      if (useNative) {
        Logger.warn("epoll unavailability cause: {}", Epoll.unavailabilityCause().getMessage());
        Logger.warn("kqueue unavailability cause: {}", KQueue.unavailabilityCause().getMessage());
      }
      Logger.info("Using NIO event loop - bossGroup={} workerGroup={}", bossGroups, workerGroups);
    }

    this.scheduledGroup = new UnorderedThreadPoolEventExecutor(scheduledGroups, new NamedThreadFactory("serverScheduledGroup"));
  }

  public Class<? extends ServerChannel> getServerChannelClass() {
    return serverChannelClass;
  }

  public Class<? extends SocketChannel> getClientChannelClass() {
    return clientChannelClass;
  }

  public EventLoopGroup getWorkerGroup() {
    return workerGroup;
  }

  public EventLoopGroup getBossGroup() {
    return bossGroup;
  }

  public EventExecutorGroup getScheduledGroup() {
    return scheduledGroup;
  }

  @Override
  public void close() throws InterruptedException {
    shutdownGracefully();
  }

  private void shutdownGracefully() throws InterruptedException {
    if (bossGroup != null) bossGroup.shutdownGracefully();
    if (workerGroup != null) workerGroup.shutdownGracefully();
    if (scheduledGroup != null) scheduledGroup.shutdownGracefully();

    // Wait until all threads are terminated.
    if (bossGroup != null) bossGroup.terminationFuture().sync();
    if (workerGroup != null) workerGroup.terminationFuture().sync();
    if (scheduledGroup != null) scheduledGroup.terminationFuture().sync();
  }

  // ================================================================================
  //  Schedule Task helpers
  // ================================================================================
  public ScheduledFuture<?> schedule(final long delay, final TimeUnit unit, final Runnable command) {
    return scheduledGroup.schedule(command, delay, unit);
  }

  public <V> ScheduledFuture<V> schedule(final long delay, final TimeUnit unit, final Callable<V> callable) {
    return scheduledGroup.schedule(callable, delay, unit);
  }

  public ScheduledFuture<?> scheduleAtFixedRate(final long period, final TimeUnit unit, final Runnable command) {
    return scheduledGroup.scheduleAtFixedRate(command, period, period, unit);
  }

  public ScheduledFuture<?> scheduleAtFixedRate(final long initialDelay, final long period,
      final TimeUnit unit, final Runnable command) {
    return scheduledGroup.scheduleAtFixedRate(command, initialDelay, period, unit);
  }
}
