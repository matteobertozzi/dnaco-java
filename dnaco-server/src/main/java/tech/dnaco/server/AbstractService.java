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

import java.net.InetSocketAddress;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannel;
import tech.dnaco.logging.LogUtil.LogLevel;
import tech.dnaco.logging.Logger;
import tech.dnaco.logging.LoggerSession;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.util.ThreadUtil;

public abstract class AbstractService {
  private ServerBootstrap bootstrap;
  private InetSocketAddress address;
  private Channel channel;

  protected AbstractService() {
    // no-op
  }

  protected void setBootstrap(final ServerBootstrap bootstrap) {
    this.bootstrap = bootstrap;
  }

  protected static ServerBootstrap newTcpServerBootstrap(final ServiceEventLoop eventLoop,
      final ChannelInitializer<SocketChannel> channelInitializer) {
    return new ServerBootstrap()
      .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
      .option(ChannelOption.SO_BACKLOG, 1024)
      .option(ChannelOption.SO_REUSEADDR, true)
      .childOption(ChannelOption.TCP_NODELAY, true)
      .group(eventLoop.getBossGroup(), eventLoop.getWorkerGroup())
      .channel(eventLoop.getServerChannelClass())
      .childHandler(channelInitializer);
  }

  public InetSocketAddress getAddress() {
    return address;
  }

  public void bind(final int port) throws InterruptedException {
    bind(new InetSocketAddress(port));
  }

  public void bind(final InetSocketAddress address) throws InterruptedException {
    this.address = address;
    this.channel = bootstrap.bind(address).sync().channel();
    Logger.info("service listening on {}", address);
  }

  public void sendStopSignal() {
    channel.close();
  }

  public void waitStopSignal() throws InterruptedException {
    channel.closeFuture().sync();
  }

  public void addShutdownHook() {
    final Thread mainThread = Thread.currentThread();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        final long startTime = System.nanoTime();
        Logger.setSession(LoggerSession.newSession(LoggerSession.SYSTEM_PROJECT_ID, "service", null, LogLevel.TRACE, 0));

        sendStopSignal();

        ThreadUtil.shutdown(mainThread);
        Logger.info("shutdown took: {}", HumansUtil.humanTimeSince(startTime));
      }
    });
  }
}
