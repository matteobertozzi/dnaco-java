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

package tech.dnaco.server.http;

import java.util.concurrent.CopyOnWriteArrayList;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;
import io.netty.handler.codec.http.cors.CorsHandler;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler.HandshakeComplete;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketServerCompressionHandler;
import io.netty.util.AttributeKey;
import tech.dnaco.logging.LogUtil.LogLevel;
import tech.dnaco.logging.Logger;
import tech.dnaco.logging.LoggerSession;
import tech.dnaco.server.AbstractService;
import tech.dnaco.server.ServiceEventLoop;
import tech.dnaco.server.stats.ServiceStats;
import tech.dnaco.server.stats.ServiceStatsHandler;
import tech.dnaco.strings.StringUtil;
import tech.dnaco.telemetry.TelemetryCollector;

public class HttpService extends AbstractService {
  private static final AttributeKey<WebSocketSession> ATTR_KEY_WS_SESSION = AttributeKey.valueOf("wsId");

  private static final CorsConfig CORS_CONFIG = CorsConfigBuilder.forAnyOrigin().allowNullOrigin()
      .allowedRequestMethods(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, HttpMethod.PATCH, HttpMethod.DELETE)
      .allowedRequestHeaders(HttpHeaderNames.CONTENT_TYPE, HttpHeaderNames.AUTHORIZATION, HttpHeaderNames.DATE)
      .build();

  private final CopyOnWriteArrayList<WebSocketListener> webSocketListeners = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<HttpListener> httpListeners = new CopyOnWriteArrayList<>();

  private final ServiceStats stats = new TelemetryCollector.Builder()
    .setName("http_service")
    .register(new ServiceStats("http_service"));

  public HttpService(final ServiceEventLoop eventLoop) {
    this(eventLoop, null);
  }

  public HttpService(final ServiceEventLoop eventLoop, final String webSocketPath) {
    this(eventLoop, 16 << 20, false, webSocketPath);
  }

  public HttpService(final ServiceEventLoop eventLoop, final int maxContentLength,
      final boolean enableCors, final String webSocketPath) {
    setBootstrap(newTcpServerBootstrap(eventLoop, new ChannelInitializer<SocketChannel>() {
      private final WebSocketFrameHandler wsHandler = new WebSocketFrameHandler(HttpService.this);
      private final HttpRequestHandler httpHandler = new HttpRequestHandler(HttpService.this);
      private final ServiceStatsHandler statsHandler = new ServiceStatsHandler(stats);

      @Override
      public void initChannel(final SocketChannel channel) throws Exception {
        final ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast(statsHandler);

        // setup http pipeline
        pipeline.addLast(new HttpServerCodec());
        pipeline.addLast(new HttpContentDecompressor());
        pipeline.addLast(new HttpObjectAggregator(maxContentLength));
        pipeline.addLast(new HttpSmartCompressor());

        // enable cors
        if (enableCors) {
          pipeline.addLast(new CorsHandler(CORS_CONFIG));
          Logger.info("CORS Enabled: {}", CORS_CONFIG);
        }

        // setup web-socket pipeline
        if (StringUtil.isNotEmpty(webSocketPath)) {
          pipeline.addLast(new WebSocketServerCompressionHandler());
          pipeline.addLast(new WebSocketServerProtocolHandler(webSocketPath, null, true));
          pipeline.addLast(wsHandler);
        }

        // http request handler
        pipeline.addLast(httpHandler);
      }
    }));
  }

  public void registerHttpListener(final HttpListener listener) {
    this.httpListeners.add(listener);
  }

  public void unregisterHttpListener(final HttpListener listener) {
    this.httpListeners.remove(listener);
  }

  public void registerWebSocketListener(final WebSocketListener listener) {
    this.webSocketListeners.add(listener);
  }

  public void unregisterWebSocketListener(final WebSocketListener listener) {
    this.webSocketListeners.remove(listener);
  }

  public interface HttpListener {
    void requestReceived(ChannelHandlerContext ctx, FullHttpRequest request);
  }

  public interface WebSocketListener {
    void connect(WebSocketSession session);
    void disconnect(WebSocketSession session);
    void textMessageReceived(WebSocketSession session, String message);
    void binaryMessageReceived(WebSocketSession session, ByteBuf data);
  }

  @Sharable
  private static final class HttpRequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private final HttpService service;

    private HttpRequestHandler(final HttpService service) {
      this.service = service;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final FullHttpRequest request) throws Exception {
      Logger.debug("http request received: {}", request.uri());
      for (HttpListener listener: service.httpListeners) {
        listener.requestReceived(ctx, request);
      }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
      Logger.setSession(LoggerSession.newSystemGeneralSession());
      Logger.error(cause, "uncaught exception");
      ctx.close();
    }
  }

  @Sharable
  private static final class WebSocketFrameHandler extends SimpleChannelInboundHandler<WebSocketFrame> {
    private final HttpService service;

    private WebSocketFrameHandler(final HttpService service) {
      this.service = service;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final WebSocketFrame frame) throws Exception {
      final WebSocketSession session = ctx.channel().attr(ATTR_KEY_WS_SESSION).get();

      Logger.debug("Received web-socket frame {}", frame);
      if (frame instanceof TextWebSocketFrame) {
        final String message = ((TextWebSocketFrame) frame).text();
        for (final WebSocketListener listener: service.webSocketListeners) {
          listener.textMessageReceived(session, message);
        }
      } else if (frame instanceof BinaryWebSocketFrame) {
        final ByteBuf content = ((BinaryWebSocketFrame) frame).content();
        for (final WebSocketListener listener: service.webSocketListeners) {
          listener.binaryMessageReceived(session, content);
        }
      } else {
        Logger.warn("unsupported frame type {}: {}", frame.getClass().getName(), frame);
      }
    }

    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) throws Exception {
      super.userEventTriggered(ctx, evt);
      Logger.debug("event triggered: {}", evt);
      if (evt instanceof HandshakeComplete) {
        final WebSocketSession session = new WebSocketSession(ctx);
        Logger.debug("new web-socket session created: {}", session);
        ctx.channel().attr(ATTR_KEY_WS_SESSION).set(session);
        for (final WebSocketListener listener: service.webSocketListeners) {
          listener.connect(session);
        }
      }
    }

    @Override
    public void channelUnregistered(final ChannelHandlerContext ctx) throws Exception {
      final WebSocketSession session = ctx.channel().attr(ATTR_KEY_WS_SESSION).get();
      Logger.debug("channel unregistered: {} {}", session, ctx.channel().remoteAddress());
      for (final WebSocketListener listener: service.webSocketListeners) {
        listener.disconnect(session);
      }

      ctx.fireChannelUnregistered();
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
      Logger.setSession(LoggerSession.newSystemGeneralSession());
      Logger.error(cause, "uncaught exception");
      ctx.close();
    }
  }

  public static void main(final String[] args) throws Exception {
    Logger.setSession(LoggerSession.newSession(LoggerSession.SYSTEM_PROJECT_ID, "service", null, LogLevel.TRACE, 0));

    try (ServiceEventLoop eloop = new ServiceEventLoop(1, Runtime.getRuntime().availableProcessors())) {
      HttpService service = new HttpService(eloop, "/websocket");
      service.bind(57025);
      service.addShutdownHook();
      service.waitStopSignal();
    }
  }
}