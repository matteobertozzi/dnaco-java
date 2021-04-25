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
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueDomainSocketChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import tech.dnaco.logging.Logger;
import tech.dnaco.threading.ShutdownUtil.StopSignal;

public class ClientEventLoop implements AutoCloseable, StopSignal {
  private final Class<? extends Channel> unixChannelClass;
  private final Class<? extends Channel> channelClass;
  private final EventLoopGroup workerGroup;

  public ClientEventLoop(final int workerGroups) {
    this(true, workerGroups);
  }

  public ClientEventLoop(final boolean useNative, final int workerGroups) {
    if (useNative && Epoll.isAvailable()) {
      workerGroup = new EpollEventLoopGroup(workerGroups, new DefaultThreadFactory("epollServerWorkerGroup"));
      unixChannelClass = EpollDomainSocketChannel.class;
      channelClass = EpollSocketChannel.class;
      Logger.info("Using epoll event loop - workerGroup={}", workerGroups);
    } else if (useNative && KQueue.isAvailable()) {
      workerGroup = new KQueueEventLoopGroup(workerGroups, new DefaultThreadFactory("kqueueServerWorkerGroup"));
      unixChannelClass = KQueueDomainSocketChannel.class;
      channelClass = KQueueSocketChannel.class;
      Logger.info("Using kqueue event loop - workerGroup={}", workerGroups);
    } else {
      workerGroup = new NioEventLoopGroup(workerGroups, new DefaultThreadFactory("nioServerWorkerGroup"));
      unixChannelClass = null;
      channelClass = NioSocketChannel.class;
      Logger.info("Using NIO event loop - workerGroup={}", workerGroups);
      if (useNative) {
        Logger.warn(Epoll.unavailabilityCause(), "epoll unavailability cause: {}");
        Logger.warn(KQueue.unavailabilityCause(), "kqueue unavailability cause: {}");
      }
    }
  }

  public Class<? extends Channel> getUnixChannelClass() {
    return unixChannelClass;
  }

  public Class<? extends Channel> getChannelClass() {
    return channelClass;
  }

  public EventLoopGroup getWorkerGroup() {
    return workerGroup;
  }

  @Override
  public void close() throws InterruptedException {
    shutdownGracefully();
  }

  @Override
  public boolean sendStopSignal() {
    if (workerGroup != null) {
      workerGroup.shutdownGracefully();
    }
    return true;
  }

  private void shutdownGracefully() throws InterruptedException {
    if (workerGroup != null) workerGroup.shutdownGracefully();

    // Wait until all threads are terminated.
    if (workerGroup != null) workerGroup.terminationFuture().sync();
  }
}
