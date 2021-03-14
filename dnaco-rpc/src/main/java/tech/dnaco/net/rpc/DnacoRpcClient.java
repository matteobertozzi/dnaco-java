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

package tech.dnaco.net.rpc;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.jctools.maps.NonBlockingHashMapLong;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import tech.dnaco.logging.Logger;
import tech.dnaco.net.AbstractClient;
import tech.dnaco.net.frame.DnacoFrameDecoder;
import tech.dnaco.net.frame.DnacoFrameEncoder;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.time.RetryUtil;
import tech.dnaco.tracing.Tracer;

public class DnacoRpcClient extends AbstractClient {
  private final CopyOnWriteArrayList<DnacoRpcClientListener> listeners = new CopyOnWriteArrayList<>();
  private final ConcurrentHashMap<ByteBuf, EventConsumer> eventSubscriptions = new ConcurrentHashMap<>();

  private final DnacoRpcObjectMapper objectMapper;

  protected DnacoRpcClient(final Bootstrap bootstrap, final RetryUtil.RetryLogic retryLogic, final DnacoRpcObjectMapper objectMapper) {
    super(bootstrap, retryLogic);
    this.objectMapper = objectMapper;
  }

  public <T> T getRpcPacketData(final DnacoRpcPacket packet, final Class<T> dataType) throws IOException {
    return objectMapper.fromBytes(packet.getData(), dataType);
  }

  public static DnacoRpcClient newTcpClient(final EventLoopGroup eloopGroup,
      final Class<? extends Channel> channelClass,
      final RetryUtil.RetryLogic retryLogic,
      final DnacoRpcObjectMapper objectMapper) {
    final Bootstrap bootstrap = newTcpClientBootstrap(eloopGroup, channelClass);

    final DnacoRpcClient client = new DnacoRpcClient(bootstrap, retryLogic, objectMapper);
    client.setupTcpServerBootstrap();

    return client;
  }

  public static DnacoRpcClient newUnixClient(final EventLoopGroup eloopGroup,
      final Class<? extends Channel> channelClass,
      final RetryUtil.RetryLogic retryLogic,
      final DnacoRpcObjectMapper objectMapper) {
    final Bootstrap bootstrap = newUnixClientBootstrap(eloopGroup, channelClass);

    final DnacoRpcClient client = new DnacoRpcClient(bootstrap, retryLogic, objectMapper);
    client.setupUnixServerBootstrap();

    return client;
  }

  public void addListener(final DnacoRpcClientListener listener) {
    listeners.add(listener);
  }

  public void removeListener(final DnacoRpcClientListener listener) {
    listeners.add(listener);
  }

  private Consumer<DnacoRpcClient> connectedHandler;
  public DnacoRpcClient whenConnected(final Consumer<DnacoRpcClient> consumer) {
    this.connectedHandler = consumer;
    return this;
  }

  // ====================================================================================================
  //  Request Related
  // ====================================================================================================
  private final AtomicLong pkgId = new AtomicLong();

  public ClientPromise<DnacoRpcResponse> sendRequest(final String requestId, final Object msg) {
    try {
      final DnacoRpcRequest packet = DnacoRpcRequest.alloc(Tracer.getCurrentTraceId(), Tracer.getCurrentSpanId(),
        pkgId.incrementAndGet(),
        Unpooled.wrappedBuffer(requestId.getBytes()),
        objectMapper.toBytes(msg, msg.getClass()),
        DnacoRpcRequest.SendResultTo.CALLER, null);
      return sendRequest(packet);
    } catch (final Throwable e) {
      return newFailedPromise(e);
    }
  }

  public ClientPromise<DnacoRpcResponse> sendRequest(final DnacoRpcRequest request) {
    final ClientPromise<DnacoRpcResponse> future = newFuture(request);
    writeAndFlush(request);
    return future;
  }

  // ====================================================================================================
  //  Event Related
  // ====================================================================================================
  public ClientPromise<Void> sendEvent(final String eventId, final Object msg) {
    return sendEvent(eventId.getBytes(StandardCharsets.UTF_8), msg);
  }

  public ClientPromise<Void> sendEvent(final byte[] eventId, final Object msg) {
    return sendEvent(Unpooled.wrappedBuffer(eventId), msg);
  }

  public ClientPromise<Void> sendEvent(final ByteBuf eventId, final Object msg) {
    try {
      final DnacoRpcEvent event = DnacoRpcEvent.alloc(Tracer.getCurrentTraceId(), Tracer.getCurrentSpanId(),
        pkgId.incrementAndGet(), 0,
        eventId,
        objectMapper.toBytes(msg, msg.getClass()));
      writeAndFlush(event);
      return newCompletedPromise(null);
    } catch (final Throwable e) {
      return newFailedPromise(e);
    }
  }

  public void subscribeToEvent(final String eventId, final EventConsumer consumer) {
    subscribeToEvent(eventId.getBytes(), consumer);
  }

  public void subscribeToEvent(final byte[] eventId, final EventConsumer consumer) {
    subscribeToEvent(Unpooled.wrappedBuffer(eventId), consumer);
  }

  public void subscribeToEvent(final ByteBuf eventId, final EventConsumer consumer) {
    this.eventSubscriptions.put(eventId, consumer);
  }

  // ====================================================================================================
  //  Network Internals
  // ====================================================================================================

  @Override
  protected void setupPipeline(final ChannelPipeline pipeline) {
    //pipeline.addLast(ZlibCodecFactory.newZlibEncoder(ZlibWrapper.GZIP));
    //pipeline.addLast(ZlibCodecFactory.newZlibDecoder(ZlibWrapper.GZIP));
    pipeline.addLast(DnacoFrameEncoder.INSTANCE);
    pipeline.addLast(DnacoRpcPacketEncoder.INSTANCE);
    pipeline.addLast(new DnacoFrameDecoder());
    pipeline.addLast(new DnacoRpcPacketDecoder());
    pipeline.addLast(new DnacoRpcClientHandler(this));
  }

  private final NonBlockingHashMapLong<InProgressClientPromise<DnacoRpcResponse>> responsesFutures = new NonBlockingHashMapLong<>();
  private ClientPromise<DnacoRpcResponse> newFuture(final DnacoRpcRequest request) {
    final InProgressClientPromise<DnacoRpcResponse> future = new InProgressClientPromise<>();
    responsesFutures.put(request.getPacketId(), future);
    return future;
  }

  private static final class DnacoRpcClientHandler extends SimpleChannelInboundHandler<DnacoRpcPacket> {
    private final DnacoRpcClient client;

    public DnacoRpcClientHandler(final DnacoRpcClient client) {
      this.client = client;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final DnacoRpcPacket msg) throws Exception {
      //Logger.debug("received packet: {}", msg);
      switch (msg.getPacketType()) {
        case REQUEST:
          handleRpcRequest(ctx, (DnacoRpcRequest)msg);
          break;
        case RESPONSE:
          handleRpcResponse(ctx, (DnacoRpcResponse)msg);
          break;
        case EVENT:
          handleRpcEvent(ctx, (DnacoRpcEvent)msg);
          break;
        case CONTROL:
        default:
          Logger.error("invalid message packet {}, closing the connection", msg.getPacketId());
          ctx.close();
          break;
      }
    }

    private void handleRpcRequest(final ChannelHandlerContext ctx, final DnacoRpcRequest request) {
      final long startNs = System.nanoTime();
      for (final DnacoRpcClientListener listener: client.listeners) {
        listener.requestReceived(client, request);
      }
      final long elapsedNs = System.nanoTime() - startNs;
      Logger.trace("request {} handled in {}", request, HumansUtil.humanTimeNanos(elapsedNs));
    }

    private void handleRpcResponse(final ChannelHandlerContext ctx, final DnacoRpcResponse response) {
      final long startNs = System.nanoTime();
      final InProgressClientPromise<DnacoRpcResponse> future = client.responsesFutures.remove(response.getPacketId());
      if (future != null) {
        future.setSuccess(response);
        final long elapsedNs = System.nanoTime() - startNs;
        Logger.trace("response {} handled in {}", response, HumansUtil.humanTimeNanos(elapsedNs));
      } else {
        Logger.warn("no response future waiting for response: {}", response);
      }
    }

    private void handleRpcEvent(final ChannelHandlerContext ctx, final DnacoRpcEvent event) {
      final long startNs = System.nanoTime();
      for (final DnacoRpcClientListener listener: client.listeners) {
        listener.eventReceived(client, event);
      }

      final EventConsumer consumer = client.eventSubscriptions.get(event.getEventId());
      if (consumer != null) consumer.eventReceived(client, event);

      final long elapsedNs = System.nanoTime() - startNs;
      Logger.trace("event {} handled in {}", event, HumansUtil.humanTimeNanos(elapsedNs));
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) {
      ctx.flush();
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) {
      client.setState(ClientState.CONNECTED);
      Logger.debug("channel registered: {}", ctx.channel().remoteAddress());
      if (client.connectedHandler != null) {
        client.connectedHandler.accept(client);
      } else {
        client.setReady();
      }
      ctx.fireChannelActive();
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) {
      client.setState(ClientState.DISCONNECTED);
      Logger.debug("channel unregistered: {}", ctx.channel().remoteAddress());
      ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
      //Logger.setSession(LoggerSession.newSystemGeneralSession());
      Logger.error(cause, "uncaught exception: {}", ctx.channel().remoteAddress());
      ctx.close();
    }
  }

  public interface DnacoRpcClientListener {
    void connect(final DnacoRpcClient client);

    void disconnect(final DnacoRpcClient client);

    void eventReceived(final DnacoRpcClient client, final DnacoRpcEvent event);
    void requestReceived(final DnacoRpcClient client, final DnacoRpcRequest request);
  }

  @FunctionalInterface
  public interface EventConsumer {
    void eventReceived(final DnacoRpcClient client, final DnacoRpcEvent event);
  }
}
