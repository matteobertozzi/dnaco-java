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

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueDomainSocketChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerDomainSocketChannel;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import tech.dnaco.logging.Logger;

public class ServiceEventLoop implements AutoCloseable {
  private final Class<? extends ServerChannel> serverUnixChannelClass;
  private final Class<? extends Channel> clientUnixChannelClass;
  private final Class<? extends ServerChannel> serverChannelClass;
  private final Class<? extends Channel> clientChannelClass;
  private final EventLoopGroup workerGroup;
  private final EventLoopGroup bossGroup;

  public ServiceEventLoop(final Class<? extends ServerChannel> serverUnixChannelClass,
      final Class<? extends Channel> clientUnixChannelClass,
      final Class<? extends ServerChannel> serverChannelClass,
      final Class<? extends Channel> clientChannelClass,
      final EventLoopGroup workerGroup,
      final EventLoopGroup bossGroup) {
    this.serverUnixChannelClass = serverUnixChannelClass;
    this.clientUnixChannelClass = clientUnixChannelClass;
    this.serverChannelClass = serverChannelClass;
    this.clientChannelClass = clientChannelClass;
    this.workerGroup = workerGroup;
    this.bossGroup = bossGroup;
  }

  public ServiceEventLoop(final int bossGroups, final int workerGroups) {
    this(true, bossGroups, workerGroups);
  }

  public ServiceEventLoop(final boolean useNative, final int bossGroups, final int workerGroups) {
    if (useNative && Epoll.isAvailable()) {
      bossGroup = new EpollEventLoopGroup(bossGroups, new DefaultThreadFactory("epollServerBossGroup"));
      workerGroup = new EpollEventLoopGroup(workerGroups, new DefaultThreadFactory("epollServerWorkerGroup"));
      serverUnixChannelClass = EpollServerDomainSocketChannel.class;
      clientUnixChannelClass = EpollDomainSocketChannel.class;
      serverChannelClass = EpollServerSocketChannel.class;
      clientChannelClass = EpollSocketChannel.class;
      Logger.info("Using epoll event loop - bossGroup={} workerGroup={}", bossGroups, workerGroups);
    } else if (useNative && KQueue.isAvailable()) {
      bossGroup = new KQueueEventLoopGroup(bossGroups, new DefaultThreadFactory("kqueueServerBossGroup"));
      workerGroup = new KQueueEventLoopGroup(workerGroups, new DefaultThreadFactory("kqueueServerWorkerGroup"));
      serverUnixChannelClass = KQueueServerDomainSocketChannel.class;
      clientUnixChannelClass = KQueueDomainSocketChannel.class;
      serverChannelClass = KQueueServerSocketChannel.class;
      clientChannelClass = KQueueSocketChannel.class;
      Logger.info("Using kqueue event loop - bossGroup={} workerGroup={}", bossGroups, workerGroups);
    } else {
      bossGroup = new NioEventLoopGroup(bossGroups, new DefaultThreadFactory("nioServerBossGroup"));
      workerGroup = new NioEventLoopGroup(workerGroups, new DefaultThreadFactory("nioServerWorkerGroup"));
      serverUnixChannelClass = null;
      clientUnixChannelClass = null;
      serverChannelClass = NioServerSocketChannel.class;
      clientChannelClass = NioSocketChannel.class;
      Logger.info("Using NIO event loop - bossGroup={} workerGroup={}", bossGroups, workerGroups);
      if (useNative) {
        Logger.warn(Epoll.unavailabilityCause(), "epoll unavailability cause: {}");
        Logger.warn(KQueue.unavailabilityCause(), "kqueue unavailability cause: {}");
      }
    }
  }

  public Class<? extends ServerChannel> getServerUnixChannelClass() {
    return serverUnixChannelClass;
  }

  public Class<? extends Channel> getClientUnixChannelClass() {
    return clientUnixChannelClass;
  }

  public boolean isUnixSupported() {
    return serverUnixChannelClass != null;
  }

  public Class<? extends ServerChannel> getServerChannelClass() {
    return serverChannelClass;
  }

  public Class<? extends Channel> getClientChannelClass() {
    return clientChannelClass;
  }

  public EventLoopGroup getWorkerGroup() {
    return workerGroup;
  }

  public EventLoopGroup getBossGroup() {
    return bossGroup;
  }

  @Override
  public void close() throws InterruptedException {
    shutdownGracefully();
  }

  private void shutdownGracefully() throws InterruptedException {
    if (bossGroup != null) bossGroup.shutdownGracefully();
    if (workerGroup != null) workerGroup.shutdownGracefully();

    // Wait until all threads are terminated.
    if (bossGroup != null) bossGroup.terminationFuture().sync();
    if (workerGroup != null) workerGroup.terminationFuture().sync();
  }
}
