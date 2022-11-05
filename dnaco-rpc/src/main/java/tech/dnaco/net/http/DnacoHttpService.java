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
package tech.dnaco.net.http;

import java.util.Date;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerKeepAliveHandler;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;
import io.netty.handler.codec.http.cors.CorsHandler;
import io.netty.util.concurrent.EventExecutorGroup;
import tech.dnaco.dispatcher.DispatchLaterException;
import tech.dnaco.dispatcher.DispatchOnShardException;
import tech.dnaco.dispatcher.Invokable;
import tech.dnaco.dispatcher.message.Message;
import tech.dnaco.dispatcher.message.MessageError;
import tech.dnaco.dispatcher.message.MessageMetadata;
import tech.dnaco.dispatcher.message.UriDispatcher.MessageTask;
import tech.dnaco.net.AbstractService;
import tech.dnaco.net.http.HttpMessageResponse.HttpMessageResponseEncoder;
import tech.dnaco.net.message.DnacoMessageHttpEncoder;

public class DnacoHttpService extends AbstractService {
  private static final int MAX_HTTP_REQUEST_SIZE = (4 << 20);

  private final HttpFrameHandler handler;
  private final CorsConfig corsConfig;

  public DnacoHttpService(final DnacoHttpServiceProcessor processor) {
    this(processor, false, null);
  }

  public DnacoHttpService(final DnacoHttpServiceProcessor processor, final boolean enableCors, final String[] corsHeaders) {
    this(processor, enableCors, corsHeaders, null);
  }

  public DnacoHttpService(final DnacoHttpServiceProcessor processor,
      final boolean enableCors, final String[] corsHeaders, final EventExecutorGroup[] shards) {
    this.handler = new HttpFrameHandler(processor);

    if (enableCors) {
      this.corsConfig = CorsConfigBuilder
        .forAnyOrigin()
        .allowNullOrigin()
        .maxAge(3600)
        .allowCredentials()
        .allowedRequestMethods(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, HttpMethod.PATCH, HttpMethod.DELETE)
        .allowedRequestHeaders("*")
        .exposeHeaders(corsHeaders)
        .build();
    } else {
      this.corsConfig = null;
    }
  }

	@Override
	protected void setupPipeline(final ChannelPipeline pipeline) {
    pipeline.addLast(new HttpServerCodec());
    pipeline.addLast(new HttpServerKeepAliveHandler());
    pipeline.addLast(new HttpContentDecompressor());
    //pipeline.addLast(new HttpServerExpectContinueHandler());
    pipeline.addLast(new HttpObjectAggregator(MAX_HTTP_REQUEST_SIZE));
    pipeline.addLast(new SmartHttpContentCompressor());
    //pipeline.addLast(new ChunkedWriteHandler());
    if (corsConfig != null) {
      pipeline.addLast(new CorsHandler(corsConfig));
    }
    pipeline.addLast(DnacoMessageHttpEncoder.INSTANCE);
    pipeline.addLast(HttpMessageResponseEncoder.INSTANCE);
    pipeline.addLast(handler);
  }

  @Sharable
  private static final class HttpFrameHandler extends ServiceChannelInboundHandler<FullHttpRequest> {
    private final DnacoHttpServiceProcessor processor;

    private HttpFrameHandler(final DnacoHttpServiceProcessor processor) {
      this.processor = processor;
    }

    @Override
    protected AbstractServiceSession sessionConnected(final ChannelHandlerContext ctx) {
      return processor.sessionConnected(ctx);
    }

    @Override
    protected void sessionDisconnected(final AbstractServiceSession session) {
      processor.sessionDisconnected(session);
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final FullHttpRequest msg) throws Exception {
      addMissingHeaders(msg);
      processor.sessionMessageReceived(ctx, msg);
    }

    private static void addMissingHeaders(final FullHttpRequest request) {
      final HttpHeaders headers = request.headers();
      if (!headers.contains(HttpHeaderNames.DATE)) {
        headers.set(HttpHeaderNames.DATE, new Date());
      }

      if (HttpUtil.isKeepAlive(request) && !headers.contains(HttpHeaderNames.CONNECTION)) {
        headers.set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
      }
    }
  }

  public interface DnacoHttpServiceProcessor {
    default AbstractServiceSession sessionConnected(final ChannelHandlerContext ctx) { return null; }
    default void sessionDisconnected(final AbstractServiceSession session) {}

    void sessionMessageReceived(ChannelHandlerContext ctx, FullHttpRequest message) throws Exception;
  }

  public static class DnacoSimpleHttpServiceProcessor implements DnacoHttpServiceProcessor {
    private final EventExecutorGroup[] shardExecutors;
    private final HttpDispatcher dispatcher;

    public DnacoSimpleHttpServiceProcessor(final HttpDispatcher dispatcher) {
      this(dispatcher, null);
    }

    public DnacoSimpleHttpServiceProcessor(final HttpDispatcher dispatcher, final EventExecutorGroup[] shardExecutors) {
      this.shardExecutors = shardExecutors;
      this.dispatcher = dispatcher;
    }

    @Override
    public void sessionMessageReceived(final ChannelHandlerContext ctx, final FullHttpRequest request) throws Exception {
      final MessageTask task = dispatcher.prepare(request);
      if (task == null) {
        handleTaskNotFound(ctx, request);
        return;
      }

      try {
        dispatcher.execute(ctx, task);
      } catch (final DispatchOnShardException e) {
        handleTaskDispatchOnshard(ctx, task, e);
      } catch (final DispatchLaterException e) {
        handleTaskDispatchLater(ctx, task, e);
      }
    }

    protected void handleTaskNotFound(final ChannelHandlerContext ctx, final FullHttpRequest request) {
      dispatcher.sendErrorMessage(ctx, request, MessageError.notFound());
    }

    protected void handleTaskDispatchOnshard(final ChannelHandlerContext ctx, final MessageTask task, final DispatchOnShardException e) {
      final EventExecutorGroup executor = shardExecutors[(e.shardHash() & 0x7fffffff) % shardExecutors.length];
      executor.submit(() -> execInvocable(ctx, task.metadata(), e.executor()));
    }

    protected void handleTaskDispatchLater(final ChannelHandlerContext ctx, final MessageTask task, final DispatchLaterException e) throws Exception {
      throw e;
    }

    private void execInvocable(final ChannelHandlerContext ctx, final MessageMetadata reqMetadata, final Invokable func) {
      try {
        final Message result = dispatcher.execute(reqMetadata, func, false, false);
        ctx.writeAndFlush(result);
      } catch (final DispatchLaterException ex) {
        final Message result = dispatcher.newErrorMessage(reqMetadata, MessageError.newInternalServerError("unexpected dispatch later"));
        ctx.writeAndFlush(result);
      }
    }
  }
}
