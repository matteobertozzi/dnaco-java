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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.channel.unix.UnixChannel;
import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.ScheduledFuture;
import tech.dnaco.logging.Logger;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.threading.ShutdownUtil.StopSignal;
import tech.dnaco.time.RetryUtil;

public abstract class AbstractClient implements StopSignal {
  public enum ClientState {
    DISCONNECTED, CONNECTED, IDENTIFICATION, READY
  }

  private final RetryUtil.RetryLogic retryLogic;
  private final Bootstrap bootstrap;

  private final AtomicReference<Channel> channelRef = new AtomicReference<>();
  private ScheduledFuture<?> scheduledRetry;
  private ClientState state = ClientState.DISCONNECTED;
  private boolean userDisconnected;
  private SocketAddress socketAddress;

  protected AbstractClient(final Bootstrap bootstrap, final RetryUtil.RetryLogic retryLogic) {
    this.retryLogic = retryLogic;
    this.bootstrap = bootstrap;
  }

  protected abstract void setupPipeline(ChannelPipeline pipeline);

  protected void setupTcpServerBootstrap() {
    bootstrap.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(final SocketChannel channel) {
        setupPipeline(channel.pipeline());
      }
    });
  }

  protected void setupUnixServerBootstrap() {
    bootstrap.handler(new ChannelInitializer<UnixChannel>() {
      @Override
      public void initChannel(final UnixChannel channel) {
        setupPipeline(channel.pipeline());
      }
    });
  }

  protected static Bootstrap newTcpClientBootstrap(final EventLoopGroup eventLoop,
      final Class<? extends Channel> channelClass) {
    return new Bootstrap()
      .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
      .option(ChannelOption.TCP_NODELAY, true)
      .group(eventLoop)
      .channel(channelClass);
  }

  protected static Bootstrap newUnixClientBootstrap(final EventLoopGroup eventLoop,
      final Class<? extends Channel> channelClass) {
    return new Bootstrap()
      .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
      .group(eventLoop)
      .channel(channelClass);
  }

  public void setState(final ClientState state) {
    final boolean ready = (this.state == ClientState.CONNECTED && state == ClientState.READY);
    this.state = state;
    if (ready) {
      writePendingFrames();
    }
  }

  public void setReady() {
    setState(ClientState.READY);
  }

  public boolean isReady() {
    return state == ClientState.READY;
  }

  public void connect(final String host, final int port) {
    connect(InetSocketAddress.createUnresolved(host, port));
  }

  public void connect(final InetAddress host, final int port) {
    connect(new InetSocketAddress(host, port));
  }

  public void connect(final File sock) throws InterruptedException {
    connect(new DomainSocketAddress(sock));
  }

  public void connect(final SocketAddress socketAddress) {
    this.socketAddress = socketAddress;
    userDisconnected = false;
    this.connect();
  }

  public boolean isConnected() {
    return channelRef.get() != null;
  }

  protected EventLoopGroup getEventLoopGroup() {
    return bootstrap.config().group();
  }

  protected void scheduleConnect() {
    if (userDisconnected) return;

    final int waitTime = retryLogic.nextWaitIntervalMillis();
    Logger.debug("reschedule connection to {} in {}", socketAddress, HumansUtil.humanTimeMillis(waitTime));
    scheduledRetry = getEventLoopGroup().schedule((Runnable) this::connect, waitTime, TimeUnit.MILLISECONDS);
  }

  protected void connect() {
    final ChannelFuture f = bootstrap.connect(socketAddress);
    f.addListener((ChannelFutureListener) connectFuture -> {
      final Channel channel = connectFuture.channel();

      scheduledRetry = null;
      if (!connectFuture.isSuccess()) {
        Logger.debug("connection failed, schedule a reconnect: {}", connectFuture.channel());
        channelRef.set(null);
        channel.close();
        AbstractClient.this.scheduleConnect();
      } else {
        retryLogic.reset();
        channelRef.set(channel);
        Logger.debug("connection succeded");

        // add retry on disconnection listener
        channel.closeFuture().addListener((ChannelFutureListener) closeFuture -> {
          Logger.debug("connection lost, schedule a reconnect");
          channelRef.set(null);
          AbstractClient.this.scheduleConnect();
        });
      }
    });
  }


  protected void write(final ReferenceCounted frame) {
    final Channel channel = channelRef.get();
    if (channel != null && isReady()) {
      channel.write(frame.retain()).addListener(newWriteFuture(frame));
    } else {
      Logger.debug("write failed add to pending frame to retry queue");
      addToPending(frame);
    }
  }

  protected void writeAndFlush(final ReferenceCounted frame) {
    final Channel channel = channelRef.get();
    if (channel != null && isReady()) {
      channel.writeAndFlush(frame.retain()).addListener(newWriteFuture(frame));
    } else {
      Logger.debug("write failed add to pending frame to retry queue");
      addToPending(frame);
    }
  }

  protected void fireEvent(final Object event) {
    final Channel channel = channelRef.get();
    if (channel != null) {
      channel.pipeline().fireUserEventTriggered(event);
    }
  }

  private final LinkedTransferQueue<ReferenceCounted> pendingFrames = new LinkedTransferQueue<>();

  protected void addToPending(final ReferenceCounted frame) {
    pendingFrames.add(frame);
  }

  protected void writePendingFrames() {
    if (pendingFrames.isEmpty()) return;

    final Channel channel = channelRef.get();
    Logger.debug("drain pending frames queue: {}", pendingFrames.size());
    while (true) {
      final ReferenceCounted frame = pendingFrames.poll();
      if (frame == null) break;

      Logger.debug("write pending frame: {}", frame);
      write(frame);
    }
    channel.flush();
  }

  private GenericFutureListener<Future<? super Void>> newWriteFuture(final ReferenceCounted frame) {
    return future -> {
      if (future.isSuccess()) {
        frame.release();
      } else {
        Logger.debug(future.cause(), "write failed add to pending frame to retry queue");
        addToPending(frame);
      }
    };
  }

  public void flush() {
    final Channel channel = channelRef.get();
    if (channel != null) channel.flush();
  }

  public void waitForShutdown() throws InterruptedException {
    final Channel channel = channelRef.get();
    if (channel != null) channel.closeFuture().sync();
  }

  public boolean disconnect() {
    Logger.debug("disconnect channel");
    userDisconnected = true;
    if (scheduledRetry != null) {
      scheduledRetry.cancel(true);
    }

    final Channel channel = channelRef.get();
    if (channel != null) {
      channel.disconnect();
      return true;
    }
    return false;
  }

  @Override
  public boolean sendStopSignal() {
    return this.disconnect();
  }

  public interface ClientPromise<T> {
    ClientPromise<T> whenComplete(BiConsumer<? super T, ? super Throwable> action);
  }

  private static final class FailedClientPromise<T> implements ClientPromise<T> {
    private final Throwable cause;

    private FailedClientPromise(final Throwable cause) {
      this.cause = cause;
    }

    @Override
    public ClientPromise<T> whenComplete(final BiConsumer<? super T, ? super Throwable> action) {
      action.accept(null, cause);
      return this;
    }
  }

  private static final class CompletedClientPromise<T> implements ClientPromise<T> {
    private final T result;

    private CompletedClientPromise(final T result) {
      this.result = result;
    }

    @Override
    public ClientPromise<T> whenComplete(final BiConsumer<? super T, ? super Throwable> action) {
      action.accept(result, null);
      return this;
    }
  }

  protected static final class InProgressClientPromise<T> implements ClientPromise<T> {
    private ArrayList<BiConsumer<? super T, ? super Throwable>> actions;
    private Throwable cause;
    private T result;
    private boolean completed;

    public InProgressClientPromise() {
      this.completed = false;
    }

    public synchronized InProgressClientPromise<T> setSuccess(final T result) {
      if (result instanceof ReferenceCounted) {
        ((ReferenceCounted)result).retain();
      }
      this.result = result;
      triggerCompletion();
      return this;
    }

    public synchronized InProgressClientPromise<T> setFailure(final Throwable cause) {
      this.cause = cause;
      triggerCompletion();
      return this;
    }

    private void triggerCompletion() {
      this.completed = true;
      if (actions != null) {
        for (final BiConsumer<? super T, ? super Throwable> action: actions) {
          action.accept(result, cause);
        }
        if (result instanceof ReferenceCounted) {
          ((ReferenceCounted)result).release();
        }
      }
    }

    @Override
    public synchronized ClientPromise<T> whenComplete(final BiConsumer<? super T, ? super Throwable> action) {
      if (completed) {
        action.accept(result, cause);

        if (result instanceof ReferenceCounted) {
          ((ReferenceCounted)result).release();
        }
      } else {
        if (actions == null) {
          actions = new ArrayList<>();
        }
        actions.add(action);
      }
      return this;
    }
  }

  protected static <T> ClientPromise<T> newFailedPromise(final Throwable cause) {
    return new FailedClientPromise<T>(cause);
  }

  protected static <T> ClientPromise<T> newCompletedPromise(final T result) {
    return new CompletedClientPromise<T>(result);
  }
}
