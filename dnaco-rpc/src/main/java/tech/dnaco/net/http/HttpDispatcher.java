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

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;
import tech.dnaco.collections.lists.ListUtil;
import tech.dnaco.data.DataFormat.DataFormatException;
import tech.dnaco.data.JsonFormat;
import tech.dnaco.dispatcher.MessageDispatcher;
import tech.dnaco.dispatcher.MethodInvoker;
import tech.dnaco.dispatcher.message.MessageMetadataMap;
import tech.dnaco.dispatcher.message.UriDispatcher;
import tech.dnaco.dispatcher.message.UriRouters;
import tech.dnaco.dispatcher.message.UriRouters.StaticFileUriRoute;
import tech.dnaco.dispatcher.message.UriRouters.UriPatternHandler;
import tech.dnaco.dispatcher.message.UriRouters.UriPatternRouter;
import tech.dnaco.dispatcher.message.UriRouters.UriRoutesBuilder;
import tech.dnaco.logging.Logger;
import tech.dnaco.net.message.DnacoMessage;
import tech.dnaco.net.message.DnacoMessageUtil;
import tech.dnaco.net.util.ByteBufDataFormatUtil;
import tech.dnaco.net.util.ExecutionId;
import tech.dnaco.strings.StringUtil;

public class HttpDispatcher extends UriDispatcher {
  private final UriPatternRouter<HttpStaticFileHandler> filesUriRouter;

  public HttpDispatcher(final UriRoutesBuilder routes) {
    this(newMessageDispatcher(), routes);
  }

  public HttpDispatcher(final MessageDispatcher messageDispatcher, final UriRoutesBuilder routes) {
    super(messageDispatcher, routes);
    this.filesUriRouter = new UriPatternRouter<>(routes.getStaticFilesUri(), this::buildStaticFileUriHandler);
  }

  private HttpStaticFileHandler buildStaticFileUriHandler(final StaticFileUriRoute route) {
    final String EXPECTED_SUFFIX = "/(.*)";
    final String uri = route.getUri();
    if (!uri.endsWith(EXPECTED_SUFFIX)) {
      throw new IllegalArgumentException("expected route ending with /(.*)");
    }

    final String uriPrefix = route.getUri().substring(0, route.getUri().length() - (EXPECTED_SUFFIX.length() - 1));
    final HttpStaticFileHandler handler = new HttpStaticFileHandler(uriPrefix, route.getStaticFileDir());
    return handler;
  }

  public boolean serveStaticFileHandler(final ChannelHandlerContext ctx, final FullHttpRequest request) {
    final UriPatternHandler<HttpStaticFileHandler> uriHandler = filesUriRouter.get(request.method().name(), request.uri());
    if (uriHandler == null) return false;

    try {
      uriHandler.getHandler().handleStaticFileRequest(ctx, uriHandler.getMatcher(), request);
    } catch (final Throwable e) {
      Logger.error(e, "failed to serve the file: {}", request.uri());
      ctx.write(buildError(0, HttpResponseStatus.INTERNAL_SERVER_ERROR));
    }
    return true;
  }

  public HttpTask prepare(final String method, final String uri) {
    final HttpCallContext ctx = new HttpCallContext(method, uri);
    final MethodInvoker methodInvoker = getRequestHandlerContext(ctx);
    return methodInvoker != null ? new HttpTask(this, methodInvoker, ctx) : null;
  }

  public HttpTask prepare(final FullHttpRequest request) {
    final HttpTask task = prepare(request.method().name(), request.uri());
    return task != null ? task.setMessage(request) : null;
  }

  public HttpTask prepare(final DnacoMessage message) {
    final HttpTask task = prepare(message.method(), message.uri());
    return task != null ? task.setMessage(message) : null;
  }

  public static class DispatchLaterException extends Exception {
    public DispatchLaterException() {
      super();
    }

    public DispatchLaterException(final Throwable cause) {
      super(null, cause);
    }

    public DispatchLaterException(final Throwable cause, final String message) {
      super(message, cause);
    }
  }

  private DnacoMessage execute(final HttpTask task) throws DispatchLaterException {
    final long packetId = task.ctx().packetId();

    final Object result;
    try {
      result = task.methodInvoker().invoke(task.ctx(), task.ctx().message);
    } catch (final DispatchLaterException e) {
      throw e;
    } catch (final DataFormatException e) {
      return buildError(packetId, HttpResponseStatus.BAD_REQUEST);
    } catch (final Throwable e) {
      if (e instanceof final DispatchLaterException dispatchLaterException) {
        Logger.error("dispatch later: {}", dispatchLaterException);
        throw dispatchLaterException;
      }
      Logger.error(e, "write internal server error");
      return buildError(packetId, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }

    if (result == null && task.methodInvoker().hasVoidResult()) {
      final MessageMetadataMap respHeaders = new MessageMetadataMap();
      respHeaders.set(DnacoMessageUtil.METADATA_FOR_HTTP_STATUS, HttpResponseStatus.NO_CONTENT.code());
      return new DnacoMessage(packetId, respHeaders, Unpooled.EMPTY_BUFFER);
    }

    if (result instanceof final DnacoMessage message) {
      return message;
    }

    try {
      final ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer();
      ByteBufDataFormatUtil.addToBytes(JsonFormat.INSTANCE, buffer, result);
      final MessageMetadataMap respHeaders = new MessageMetadataMap();
      respHeaders.set(DnacoMessageUtil.METADATA_FOR_HTTP_STATUS, HttpResponseStatus.OK.code());
      respHeaders.set(DnacoMessageUtil.METADATA_CONTENT_LENGTH, buffer.readableBytes());
      return new DnacoMessage(packetId, respHeaders, buffer);
    } catch (final Throwable e) {
      return buildError(packetId, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private DnacoMessage buildError(final long packetId, final HttpResponseStatus status) {
    final MessageMetadataMap metadata = new MessageMetadataMap();
    metadata.set(DnacoMessageUtil.METADATA_FOR_HTTP_STATUS, status.code());
    return new DnacoMessage(packetId, metadata, Unpooled.EMPTY_BUFFER);
  }

  private static final String HEADER_KEY_ACCEPT = "accept";

  public static class HttpTask {
    private final HttpDispatcher dispatcher;
    private final MethodInvoker methodInvoker;
    private final HttpCallContext ctx;

    protected HttpTask (final HttpTask other) {
      this(other.dispatcher, other.methodInvoker, other.ctx);
    }

    private HttpTask (final HttpDispatcher dispatcher, final MethodInvoker methodInvoker, final HttpCallContext ctx) {
      this.dispatcher = dispatcher;
      this.methodInvoker = methodInvoker;
      this.ctx = ctx;
    }

    public HttpTask setMessage(final FullHttpRequest request) {
      return setMessage(new DnacoMessage(0, new MessageMetadataMap(request.headers().entries()), request.content()));
    }

    public HttpTask setMessage(final DnacoMessage message) {
      ctx.setMessage(message);
      return this;
    }

    private MethodInvoker methodInvoker() {
      return methodInvoker;
    }

    private HttpCallContext ctx() {
      return ctx;
    }

    public ExecutionId executionId() {
      return ctx.executionId();
    }

    public long packetId() {
      return ctx.packetId();
    }

    public boolean hasAnnotation(final Class<? extends Annotation> annotationType) {
      return methodInvoker.hasAnnotation(annotationType);
    }

    public DnacoMessage execute() throws DispatchLaterException {
      return dispatcher.execute(this);
    }
  }

  // ================================================================================
  // PROTECTED mapping and execute
  // ================================================================================
  protected MethodInvoker getRequestHandlerContext(final HttpCallContext ctx) {
    final int methodMask = UriRouters.uriMethodMask(ctx.method());
    final String path = ctx.path();
    if (StringUtil.isEmpty(path)) return null;

    final MethodInvoker handler = getRequestHandler(ctx, methodMask, path);
    return handler;
  }

  public static class HttpCallContext extends MessageCallContext {
    private final ExecutionId executionId;
    private final Map<String, List<String>> queryParams;
    private final String method;
    private final String path;

    private DnacoMessage message = null;

    protected HttpCallContext(final HttpCallContext other) {
      super(other);
      this.executionId = other.executionId;
      this.queryParams = other.queryParams;
      this.method = other.method;
      this.path = other.path;
      this.message = other.message;
    }

    public HttpCallContext(final String method, final String uri) {
      final QueryStringDecoder queryDecoder = new QueryStringDecoder(uri);

      this.executionId = ExecutionId.randomId();
      this.method = method;
      this.path = queryDecoder.path();
      this.queryParams = queryDecoder.parameters();
    }

    private void setMessage(final DnacoMessage message) {
      this.message = message;
    }

    public DnacoMessage message() { return message; }

    public long packetId() { return message.packetId(); }
    public ExecutionId executionId() { return executionId; }

    public String method() { return method; }
    public String path() { return path; }

    // --------------------------------------------------
    //  body
    // --------------------------------------------------
    public ByteBuf body() { return message.data(); }

    // --------------------------------------------------
    //  headers
    // --------------------------------------------------
    public String headerValue(final String key) {
      return message.metadataMap().get(key);
    }

    public List<String> headerValueAsList(final String key) {
      return message.metadataMap().getList(key);
    }

    // --------------------------------------------------
    //  query params
    // --------------------------------------------------
    public String queryParam(final String key) {
      final List<String> values = queryParams.get(key);
      return ListUtil.isNotEmpty(values) ? values.get(0) : null;
    }

    public List<String> queryParamAsList(final String key) {
      return queryParams.get(key);
    }
  }

  // ================================================================================
  //  PRIVATE HTTP Message Dispatcher
  // ================================================================================
  /*
  private static class FormEncodedParamParser extends BodyParamParser {
    private FormEncodedParamParser(final Parameter param, final Annotation annotation) {
      super(param, annotation);
    }

    @Override
    protected Object parseBody(final MessageCallContext context, final MessageData message) throws Exception {
      return parseBody(message);
    }

    protected static Map<String, String> parseBody(final MessageData message) throws IOException {
      final FullHttpRequest request = DnacoMessageHttpEncoder.encodeAsRequest(message);
      try {
        final HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(request);
        final HashMap<String, String> params = new HashMap<>();
        try {
          while (decoder.hasNext()) {
            final InterfaceHttpData data = decoder.next();
            if (data.getHttpDataType() == HttpDataType.Attribute) {
              final Attribute attribute = (Attribute)data;
              params.put(attribute.getName(), attribute.getValue());
            }
          }
        } catch (final HttpPostRequestDecoder.EndOfDataDecoderException e) {
          // no-op (EOF)
          decoder.destroy();
        }
        return params;
      } finally {
        request.release();
      }
    }
  }
  */
}
