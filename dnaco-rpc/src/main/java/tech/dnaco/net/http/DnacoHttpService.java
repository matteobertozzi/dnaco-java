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

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerKeepAliveHandler;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;
import io.netty.handler.codec.http.cors.CorsHandler;
import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.collections.lists.ListUtil;
import tech.dnaco.data.CborFormat;
import tech.dnaco.data.DataFormat;
import tech.dnaco.data.JsonFormat;
import tech.dnaco.logging.LogEntry;
import tech.dnaco.logging.LogEntryData;
import tech.dnaco.logging.Logger;
import tech.dnaco.net.AbstractService;
import tech.dnaco.net.http.DnacoHttpHandler.QueryParam;
import tech.dnaco.net.http.DnacoHttpHandler.UriMapping;
import tech.dnaco.net.http.DnacoHttpHandler.UriPrefix;
import tech.dnaco.net.util.ByteBufDataFormatUtil;
import tech.dnaco.net.util.RemoteAddress;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.strings.StringUtil;
import tech.dnaco.tracing.Span;
import tech.dnaco.tracing.Tracer;

public class DnacoHttpService extends AbstractService {
  private static final int MAX_HTTP_REQUEST_SIZE = (4 << 20);

  private static final String X_HEADER_TRACE_ID = "X-TraceId";
  private static final String X_HEADER_SPAN_ID = "X-SpanId";
  private static final String X_HEADER_TOTAL_TIME = "X-TotalTime";

  private final UriMappings uriMappings = new UriMappings();

  private final HttpFrameHandler handler;
  private final CorsConfig corsConfig;

  public DnacoHttpService() {
    this(false);
  }

  public DnacoHttpService(final boolean enableCors) {
    this.handler = new HttpFrameHandler(uriMappings);

    if (enableCors) {
      this.corsConfig = CorsConfigBuilder.forAnyOrigin().allowNullOrigin()
        .maxAge(3600)
        .allowCredentials()
        .allowedRequestMethods(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, HttpMethod.PATCH, HttpMethod.DELETE)
        .allowedRequestHeaders("*")
        .exposeHeaders(X_HEADER_TRACE_ID, X_HEADER_SPAN_ID, X_HEADER_TOTAL_TIME)
        .build();
    } else {
      this.corsConfig = null;
    }
  }

	@Override
	protected void setupPipeline(final ChannelPipeline pipeline) {
    pipeline.addLast(new HttpServerCodec());
    pipeline.addLast(new HttpContentDecompressor());
    pipeline.addLast(new HttpServerKeepAliveHandler());
    pipeline.addLast(new HttpObjectAggregator(MAX_HTTP_REQUEST_SIZE));
    pipeline.addLast(new HttpContentCompressor());
    if (corsConfig != null) {
      pipeline.addLast(new CorsHandler(corsConfig));
    }
    pipeline.addLast(handler);
  }

  @Sharable
  private static final class HttpFrameHandler extends ServiceChannelInboundHandler<FullHttpRequest> {
    private final UriMappings uriHandlers;

    private HttpFrameHandler(final UriMappings uriHandlers) {
      this.uriHandlers = uriHandlers;
    }

    @Override
    protected AbstractServiceSession sessionConnected(final ChannelHandlerContext ctx) {
      return null;
    }

    @Override
    protected void sessionDisconnected(final AbstractServiceSession session) {
      // no-op
    }

  	@Override
    protected void channelRead0(final ChannelHandlerContext ctx, final FullHttpRequest request) throws Exception {
      final String traceId = request.headers().get(X_HEADER_TRACE_ID);
      final String parentSpanId = request.headers().get(X_HEADER_SPAN_ID);

      try (Span span = Tracer.newSubTask(traceId, parentSpanId)) {
        final String address = RemoteAddress.getRemoteAddress(ctx.channel(), request);
        logToSysHttp(buildEntry(request, address));

        final boolean keepAlive = HttpUtil.isKeepAlive(request);
        final QueryStringDecoder queryDecoder = new QueryStringDecoder(request.uri());
        final HttpHandler handler = uriHandlers.getHttpHandler(request.method().name(), queryDecoder.path());
        if (handler != null) {
          handleRequest(ctx, keepAlive, request, queryDecoder, handler);
        } else {
          Logger.warn("{} {} - keepAlive {} - INVALID REQUEST", request.method(), request.uri(), keepAlive);
          writeResponse(ctx, request, keepAlive, HttpResponseStatus.NOT_FOUND);
        }
      }
    }

    private void handleRequest(final ChannelHandlerContext ctx, final boolean keepAlive,
        final FullHttpRequest request, final QueryStringDecoder queryDecoder, final HttpHandler handler) {
      try {
        final Object result = handler.invoke(request, queryDecoder);
        if (result == null) {
          writeResponse(ctx, request, keepAlive, HttpResponseStatus.OK, null, Unpooled.EMPTY_BUFFER);
          return;
        }

        if (result instanceof final HttpResponse response) {
          // TODO: headers
          writeResponse(ctx, request, keepAlive, response.getStatus(), null, response.getBody());
          return;
        }

        final String contentType;
        final ByteBuf buf = ctx.alloc().buffer();
        final String accept = request.headers().get(HttpHeaderNames.ACCEPT);
        final DataFormat dataFormat;
        if (StringUtil.contains(accept, CONTENT_TYPE_CBOR)) {
          dataFormat = CborFormat.INSTANCE;
          contentType = CONTENT_TYPE_CBOR;
        } else {
          dataFormat = JsonFormat.INSTANCE;
          contentType = CONTENT_TYPE_JSON;
        }
        ByteBufDataFormatUtil.addToBytes(dataFormat, buf, result);
        writeResponse(ctx, request, keepAlive, HttpResponseStatus.OK, contentType, buf);
      } catch (final Throwable e) {
        Logger.error(e, "failed to execute request: {} {}", request.method(), request.uri());
        writeResponse(ctx, request, keepAlive, HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
    }

    private static void writeResponse(final ChannelHandlerContext ctx,
        final FullHttpRequest request, final boolean keepAlive,
        final HttpResponseStatus status) {
      writeResponse(ctx, request, keepAlive, status, null, Unpooled.EMPTY_BUFFER);
    }

    private static void writeResponse(final ChannelHandlerContext ctx,
        final FullHttpRequest request, final boolean keepAlive,
        final HttpResponseStatus status, final String contentType, final ByteBuf content) {
      final FullHttpResponse response = buildResponse(keepAlive, status, content);
      if (StringUtil.isNotEmpty(contentType)) {
        response.headers().add(HttpHeaderNames.CONTENT_TYPE, contentType);
      }
      response.headers().add(X_HEADER_TRACE_ID, Tracer.getCurrentTraceId());
      response.headers().add(X_HEADER_SPAN_ID, Tracer.getCurrentSpanId());
      logToSysHttp(buildEntry(request, response));
      ctx.write(response);
    }
  }

  private static FullHttpResponse buildResponse(final boolean keepAlive, final HttpResponseStatus status) {
    return buildResponse(keepAlive, status, Unpooled.EMPTY_BUFFER);
  }

  private static FullHttpResponse buildResponse(final boolean keepAlive,
      final HttpResponseStatus status, final ByteBuf content) {
    final FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);
    response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
    buildResponse(keepAlive, response);
    return response;
  }

  private static void buildResponse(final boolean keepAlive, final FullHttpResponse response) {
    response.headers().set(HttpHeaderNames.DATE, new Date());

    if (keepAlive && response.status() != HttpResponseStatus.INTERNAL_SERVER_ERROR) {
      response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
    } else {
      response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
    }
  }

  private static void logToSysHttp(final LogEntry entry) {
    final Thread thread = Thread.currentThread();
    entry.setTenantId("__SYS_HTTP__");
    entry.setThread(thread.getName());
    entry.setModule("http");
    entry.setOwner("trace");
    entry.setTimestamp(System.currentTimeMillis());
    entry.setTraceId(Tracer.getCurrentTraceId());
    entry.setSpanId(Tracer.getCurrentSpanId());
    Logger.addRaw(thread, entry);
  }

  // ================================================================================
  //  Http (non RPC) handlers
  // ================================================================================
  private static final String CONTENT_TYPE_CBOR = "application/cbor";
  private static final String CONTENT_TYPE_JSON = "application/json";

  public void addHandler(final DnacoHttpHandler handler) {
    final Method[] methods = handler.getClass().getMethods();
    if (ArrayUtil.isEmpty(methods)) {
      return;
    }

    for (int i = 0; i < methods.length; ++i) {
      final Method method = methods[i];
      if (method.isAnnotationPresent(UriMapping.class)) {
        uriMappings.addUriMapping(handler, method);
      }
    }
  }

  private static final class UriMappings {
    private final HashMap<String, HttpHandler> httpUriHandlers = new HashMap<>();

    public HttpHandler getHttpHandler(final String method, final String uri) {
      System.out.println("FETCH MAPPING: " + method + "." + uri);
      return httpUriHandlers.get(method + "." + uri);
    }

    private void addUriMapping(final DnacoHttpHandler handler, final Method method) {
      final UriPrefix uriPrefix = handler.getClass().getAnnotation(UriPrefix.class);
      final UriMapping uriMapping = method.getAnnotation(UriMapping.class);
      final String[] httpMethods = uriMapping.method();

      for (int i = 0; i < httpMethods.length; ++i) {
        final String httpMethod = httpMethods[i];
        final String uri = httpMethod + "." + (uriPrefix != null ? uriPrefix.value() : "") + uriMapping.uri();
        this.httpUriHandlers.put(uri, new HttpHandler(handler, method));
        System.out.println("ADD MAPPING: " + uri);
      }
    }
  }

  private static final class HttpHandler {
    private final ParamMapper[] paramMappers;
    private final DnacoHttpHandler handler;
    private final Method method;
    private final boolean hasResult;

    private HttpHandler(final DnacoHttpHandler handler, final Method method) {
      this.handler = handler;
      this.method = method;

      final Parameter[] params = method.getParameters();
      this.paramMappers = new ParamMapper[params.length];

      String bodyParam = null;
      for (int i = 0; i < params.length; ++i) {
        final Parameter param = params[i];
        if (param.isAnnotationPresent(QueryParam.class)) {
          final QueryParam queryParam = params[i].getAnnotation(QueryParam.class);
          paramMappers[i] = new QueryParamConverter(queryParam, param.getType());
        } else if (bodyParam != null) {
          throw new IllegalArgumentException("already got a body (non annotated) argument " + bodyParam + ", " + param.getName() + " cannot be a body too");
        } else {
          bodyParam = param.getName();
          paramMappers[i] = new BodyConverter(param.getType());
        }
      }

      this.hasResult = method.getReturnType() != Void.class && method.getReturnType() != void.class;
    }

    public Object invoke(final FullHttpRequest request, final QueryStringDecoder queryDecoder) throws Exception {
      final Object[] params = new Object[paramMappers.length];
      for (int i = 0; i < params.length; ++i) {
        params[i] = paramMappers[i].convert(request, queryDecoder);
      }

      return method.invoke(handler, params);
    }

    private interface ParamMapper {
      Object convert(final FullHttpRequest request, QueryStringDecoder queryDecoder) throws IOException;
    }

    private static final class QueryParamConverter implements ParamMapper {
      private final Class<?> valueType;
      private final String name;
      private final Object defaultValue;

      private QueryParamConverter(final QueryParam queryParam, final Class<?> valueType) {
        this.valueType = valueType;
        this.name = queryParam.name();
        try {
          if (valueType == String.class) {
            this.defaultValue = queryParam.defaultValue();
          } else if (StringUtil.isNotEmpty(queryParam.defaultValue())) {
            this.defaultValue = JsonFormat.INSTANCE.fromString(queryParam.defaultValue(), valueType);
          } else {
            this.defaultValue = null;
          }
        } catch (final Throwable e) {
          throw new IllegalArgumentException("unable to convert " + queryParam.name() + " defaultValue " + queryParam.defaultValue() + " to " + valueType);
        }
      }

      @Override
      public Object convert(final FullHttpRequest request, final QueryStringDecoder queryDecoder) throws IOException {
        final List<String> values = queryDecoder.parameters().get(name);
        if (ListUtil.isEmpty(values)) {
          return defaultValue;
        }
        return JsonFormat.INSTANCE.fromString(values.get(0), valueType);
      }
    }

    private static final class BodyConverter implements ParamMapper {
      private final Class<?> valueType;

      private BodyConverter(final Class<?> valueType) {
        this.valueType = valueType;
      }

      @Override
      public Object convert(final FullHttpRequest request, final QueryStringDecoder queryDecoder) throws IOException {
        final String contentType = request.headers().get(HttpHeaderNames.CONTENT_TYPE);
        final DataFormat dataFormat;
        if (StringUtil.equals(contentType, CONTENT_TYPE_CBOR)) {
          dataFormat = CborFormat.INSTANCE;
        } else {
          dataFormat = JsonFormat.INSTANCE;
        }
        return ByteBufDataFormatUtil.fromBytes(dataFormat, request.content(), valueType);
      }
    }
  }

  private static LogEntryData buildEntry(final FullHttpRequest request, final String address) {
    final int bodyLength = request.content() != null ? request.content().readableBytes() : 0;

    final LogEntryData entry = new LogEntryData();
    entry.setLabel("HTTP-REQUEST: " + request.method() + " " + request.uri());

    // headers
    int index = 0;
    final HttpHeaders headers = request.headers();
    final Object[] headerKvs = new Object[2 * (5 + headers.size())];
    headerKvs[index++] = "REQ-METHOD";
    headerKvs[index++] = request.method();
    headerKvs[index++] = "REQ-URI";
    headerKvs[index++] = request.uri();
    headerKvs[index++] = "REQ-IP";
    headerKvs[index++] = address;
    headerKvs[index++] = "REQ-LENGTH";
    headerKvs[index++] = HumansUtil.humanSize(bodyLength);
    headerKvs[index++] = null;
    headerKvs[index++] = null;
    for (final Entry<String, String> hentry: headers) {
      headerKvs[index++] = hentry.getKey();
      headerKvs[index++] = hentry.getValue();
    }
    entry.addKeyValues(headerKvs);

    // body
    entry.addText(request.content().toString(StandardCharsets.UTF_8));

    return entry;
  }

  private static LogEntryData buildEntry(final FullHttpRequest request, final FullHttpResponse response) {
    final LogEntryData entry = new LogEntryData();
    if (request != null) {
      entry.setLabel("HTTP-RESPONSE: " + request.method() + " " + request.uri());
    }

    // headers
    int index = 0;
    final HttpHeaders headers = response.headers();
    final Object[] headerKvs = new Object[2 * headers.size()];
    for (final Entry<String, String> hentry: headers) {
      headerKvs[index++] = hentry.getKey();
      headerKvs[index++] = hentry.getValue();
    }
    entry.addKeyValues(headerKvs);

    // body
    final int bodyLength = response.content() != null ? response.content().readableBytes() : 0;
    if (bodyLength > 0) {
      final String contentType = response.headers().get(HttpHeaderNames.CONTENT_TYPE);
      if (StringUtil.contains(StringUtil.toLower(contentType), HttpHeaderValues.APPLICATION_JSON.toString())) {
        entry.addJson(response.content().toString(StandardCharsets.UTF_8));
      } else {
        entry.addText(response.content().toString(StandardCharsets.UTF_8));
      }
    }

    return entry;
  }
}
