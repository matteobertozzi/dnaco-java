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
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponse;
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
import tech.dnaco.logging.Logger;
import tech.dnaco.net.AbstractService;
import tech.dnaco.net.http.HttpMessageFileResponse.HttpMessageFileResponseEncoder;
import tech.dnaco.net.http.HttpMessageResponse.HttpMessageResponseEncoder;
import tech.dnaco.net.message.DnacoMessageHttpEncoder;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.telemetry.ConcurrentHistogram;
import tech.dnaco.telemetry.ConcurrentTimeRangeCounter;
import tech.dnaco.telemetry.ConcurrentTopK;
import tech.dnaco.telemetry.CounterMap;
import tech.dnaco.telemetry.Histogram;
import tech.dnaco.telemetry.TelemetryCollector;
import tech.dnaco.telemetry.TopK.TopType;
import tech.dnaco.tracing.Span;
import tech.dnaco.tracing.Tracer;

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
    addNetworkIoStats(pipeline);
    pipeline.addLast(new HttpServerCodec());
    pipeline.addLast(new HttpServerKeepAliveHandler());
    pipeline.addLast(new HttpContentDecompressor());
    //pipeline.addLast(new HttpServerExpectContinueHandler());
    pipeline.addLast(new HttpObjectAggregator(MAX_HTTP_REQUEST_SIZE));
    pipeline.addLast(new SmartHttpContentCompressor());
    pipeline.addLast(HttpResponseStats.INSTANCE);
    //pipeline.addLast(new ChunkedWriteHandler());
    if (corsConfig != null) {
      pipeline.addLast(new CorsHandler(corsConfig));
    }
    pipeline.addLast(DnacoMessageHttpEncoder.INSTANCE);
    pipeline.addLast(HttpMessageResponseEncoder.INSTANCE);
    pipeline.addLast(HttpMessageFileResponseEncoder.INSTANCE);
    pipeline.addLast(handler);
  }

  @Sharable
  private static final class HttpResponseStats extends ChannelOutboundHandlerAdapter {
    private static final HttpResponseStats INSTANCE = new HttpResponseStats();

    private final CounterMap httpStatusCodes = new TelemetryCollector.Builder()
      .setName("http_service_status_codes")
      .setLabel("Http Service Responses Status Codes")
      .register(new CounterMap(32));

    private final ConcurrentTimeRangeCounter httpOk = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_COUNT)
      .setName("http_service_responses_ok")
      .setLabel("Http 200/204 OK Responses")
      .register(new ConcurrentTimeRangeCounter(24 * 60, 1, TimeUnit.MINUTES));

    private final ConcurrentTimeRangeCounter http3xx = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_COUNT)
      .setName("http_service_responses_3xx")
      .setLabel("Http 3xx Responses")
      .register(new ConcurrentTimeRangeCounter(24 * 60, 1, TimeUnit.MINUTES));

    private final ConcurrentTimeRangeCounter http4xx = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_COUNT)
      .setName("http_service_responses_4xx")
      .setLabel("Http 4xx Responses")
      .register(new ConcurrentTimeRangeCounter(24 * 60, 1, TimeUnit.MINUTES));

    private final ConcurrentTimeRangeCounter http5xx = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_COUNT)
      .setName("http_service_responses_5xx")
      .setLabel("Http 5xx Responses")
      .register(new ConcurrentTimeRangeCounter(24 * 60, 1, TimeUnit.MINUTES));

    private final ConcurrentTimeRangeCounter httpOthers = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_COUNT)
      .setName("http_service_responses_others")
      .setLabel("Http others Responses")
      .register(new ConcurrentTimeRangeCounter(24 * 60, 1, TimeUnit.MINUTES));

    private final ConcurrentHistogram responseBodySizeHisto = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_SIZE)
      .setName("http_service_response_body_size_histo")
      .setLabel("Http Service Response Body Size")
      .register(new ConcurrentHistogram(Histogram.DEFAULT_SIZE_BOUNDS));

    private HttpResponseStats() {
      // no-op
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) throws Exception {
      computeHttpResponseStats(msg);
      super.write(ctx, msg, promise);
    }

    private void computeHttpResponseStats(final Object msg) {
      if (msg instanceof final FullHttpResponse httpResponse) {
        final ByteBuf body = httpResponse.content();
        responseBodySizeHisto.add(body != null ? body.readableBytes() : 0);
        computeHttpResponseStats(httpResponse);
      } else if (msg instanceof final HttpResponse httpResponse) {
        computeHttpResponseStats(httpResponse);
      } else {
        Logger.warn("unhandled HTTP WRITE type:{} -> {}", msg.getClass(), msg);
      }
    }

    private void computeHttpResponseStats(final HttpResponse httpResponse) {
      httpStatusCodes.inc(httpResponse.status().toString());

      final int statusCode = httpResponse.status().code();
      if (statusCode == 200 || statusCode == 204) {
        httpOk.inc();
      } else if (statusCode >= 400 && statusCode <= 499) {
        http4xx.inc();
      } else if (statusCode >= 500 && statusCode <= 599) {
        http5xx.inc();
      } else if (statusCode >= 300 && statusCode <= 399) {
        http3xx.inc();
      } else {
        httpOthers.inc();
      }
    }
  }

  @Sharable
  private static final class HttpFrameHandler extends ServiceChannelInboundHandler<FullHttpRequest> {
    private final ConcurrentTimeRangeCounter requestCount = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_COUNT)
      .setName("http_service_request_count")
      .setLabel("Http Service Request count")
      .register(new ConcurrentTimeRangeCounter(60, 1, TimeUnit.MINUTES));

    private final ConcurrentHistogram requestBodySizeHisto = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_SIZE)
      .setName("http_service_request_body_size_histo")
      .setLabel("Http Service Request Body Size")
      .register(new ConcurrentHistogram(Histogram.DEFAULT_SIZE_BOUNDS));

    private final ConcurrentTopK topRequests = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_COUNT)
      .setName("http_service_top_requests")
      .setLabel("Http Service Top Requests")
      .register(new ConcurrentTopK(TopType.COUNT, 16));

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
      try (Span span = Tracer.createSpan(msg.headers().get("X-Goal-TraceId"))) {
        computeHttpResponseStats(msg);

        addMissingHeaders(msg);
        processor.sessionMessageReceived(ctx, msg);
      }
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

    private void computeHttpResponseStats(final FullHttpRequest request) {
      requestCount.inc();

      final ByteBuf body = request.content();
      requestBodySizeHisto.add(body != null ? body.readableBytes() : 0);

      final String uri = request.uri();
      final int paramsIndex = uri.indexOf('?');
      topRequests.add((paramsIndex < 0) ? uri : uri.substring(0, paramsIndex), 0);
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
