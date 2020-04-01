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

package tech.dnaco.server.text;

import java.util.concurrent.CopyOnWriteArrayList;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.AttributeKey;
import tech.dnaco.logging.LogUtil.LogLevel;
import tech.dnaco.logging.Logger;
import tech.dnaco.logging.LoggerSession;
import tech.dnaco.server.AbstractService;
import tech.dnaco.server.ServiceEventLoop;
import tech.dnaco.server.stats.ServiceStats;
import tech.dnaco.server.stats.ServiceStatsHandler;
import tech.dnaco.server.stats.ServiceStatsHandler.ChannelStats;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.telemetry.TelemetryCollector;

public class TextService extends AbstractService {
  private static final AttributeKey<TextServiceSession> ATTR_KEY_SESSION = AttributeKey.valueOf("sid");

  private final CopyOnWriteArrayList<TextServiceListener> listeners = new CopyOnWriteArrayList<>();
  private final ServiceStats stats = new TelemetryCollector.Builder()
    .setName("text_service")
    .register(new ServiceStats("text_service"));

  public TextService(final ServiceEventLoop eventLoop) {
    this(eventLoop, 1 << 10);
  }

  public TextService(final ServiceEventLoop eventLoop, final int maxContentLength) {
    setBootstrap(newTcpServerBootstrap(eventLoop, new ChannelInitializer<SocketChannel>() {
      private final TextServiceHandler handler = new TextServiceHandler(TextService.this);
      private final ServiceStatsHandler statsHandler = new ServiceStatsHandler(stats);
      private final StringDecoder decoder = new StringDecoder();
      private final StringEncoder encoder = new StringEncoder();

      @Override
      public void initChannel(final SocketChannel channel) throws Exception {
        final ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast(statsHandler);
        pipeline.addLast(new DelimiterBasedFrameDecoder(maxContentLength, Delimiters.lineDelimiter()));
        pipeline.addLast(decoder);
        pipeline.addLast(encoder);
        pipeline.addLast(handler);
      }
    }));
  }

  public void registerListener(final TextServiceListener listener) {
    this.listeners.add(listener);
  }

  public void unregisterListener(final TextServiceListener listener) {
    this.listeners.remove(listener);
  }

  public interface TextServiceListener {
    void connect(TextServiceSession session);
    void disconnect(TextServiceSession session);
    void messageReceived(TextServiceSession session, String message);
  }

  @Sharable
  private static final class TextServiceHandler extends SimpleChannelInboundHandler<String> {
    private final TextService service;

    private TextServiceHandler(final TextService service) {
      this.service = service;
    }

    @Override
    public void channelRead0(final ChannelHandlerContext ctx, final String message) {
      final TextServiceSession session = ctx.channel().attr(ATTR_KEY_SESSION).get();
      final ChannelStats channelStats = ServiceStatsHandler.getChannelStats(ctx);

      Logger.debug("read: {}", HumansUtil.humanTimeNanos(channelStats.markReadAsComplete()));
      for (TextServiceListener listener: service.listeners) {
        listener.messageReceived(session, message);
      }
      ctx.write(message);
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) {
      ctx.flush();
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
      Logger.debug("client connected: {}", ctx.channel().remoteAddress());

      final TextServiceSession session = new TextServiceSession(ctx);
      ctx.channel().attr(ATTR_KEY_SESSION).set(session);
      for (TextServiceListener listener: service.listeners) {
        listener.connect(session);
      }

      ctx.fireChannelActive();
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
      Logger.debug("client disconnected: {}", ctx.channel().remoteAddress());

      final TextServiceSession session = ctx.channel().attr(ATTR_KEY_SESSION).getAndSet(null);
      for (TextServiceListener listener: service.listeners) {
        listener.disconnect(session);
      }

      ctx.fireChannelInactive();
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
      final TextService service = new TextService(eloop);
      service.bind(57025);
      service.addShutdownHook();
      service.waitStopSignal();
    }
  }
}
