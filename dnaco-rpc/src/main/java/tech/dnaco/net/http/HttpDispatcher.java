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
import java.lang.annotation.Annotation;
import java.lang.reflect.Parameter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.multipart.Attribute;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.handler.codec.http.multipart.InterfaceHttpData.HttpDataType;
import tech.dnaco.collections.lists.ListUtil;
import tech.dnaco.collections.maps.MapUtil;
import tech.dnaco.data.CborFormat;
import tech.dnaco.data.DataFormat;
import tech.dnaco.data.DataFormat.DataFormatException;
import tech.dnaco.data.JsonFormat;
import tech.dnaco.data.XmlFormat;
import tech.dnaco.data.json.JsonUtil;
import tech.dnaco.logging.Logger;
import tech.dnaco.net.dispatcher.CallContext;
import tech.dnaco.net.dispatcher.MessageDispatcher;
import tech.dnaco.net.dispatcher.MethodInvoker;
import tech.dnaco.net.dispatcher.ParamParser;
import tech.dnaco.net.http.HttpHandler.CborBody;
import tech.dnaco.net.http.HttpHandler.FormEncodedBody;
import tech.dnaco.net.http.HttpHandler.HeaderValue;
import tech.dnaco.net.http.HttpHandler.JsonBody;
import tech.dnaco.net.http.HttpHandler.QueryParam;
import tech.dnaco.net.http.HttpHandler.UriVariable;
import tech.dnaco.net.http.HttpHandler.XmlBody;
import tech.dnaco.net.http.HttpRouters.StaticFileUriRoute;
import tech.dnaco.net.http.HttpRouters.UriPatternHandler;
import tech.dnaco.net.http.HttpRouters.UriPatternRouter;
import tech.dnaco.net.http.HttpRouters.UriRoute;
import tech.dnaco.net.http.HttpRouters.UriRoutesBuilder;
import tech.dnaco.net.http.HttpRouters.UriStaticRouter;
import tech.dnaco.net.http.HttpRouters.UriVariableHandler;
import tech.dnaco.net.http.HttpRouters.UriVariableRouter;
import tech.dnaco.net.message.DnacoMessage;
import tech.dnaco.net.message.DnacoMessageHttpEncoder;
import tech.dnaco.net.message.DnacoMessageUtil;
import tech.dnaco.net.message.DnacoMetadataMap;
import tech.dnaco.net.util.ByteBufDataFormatUtil;
import tech.dnaco.net.util.ExecutionId;
import tech.dnaco.strings.StringUtil;

public class HttpDispatcher {
  private final MessageDispatcher messageDispatcher;

  private final UriStaticRouter<MethodInvoker> staticUriRouter;
  private final UriVariableRouter<MethodInvoker> variableUriRouter;
  private final UriPatternRouter<MethodInvoker> patternUriRouter;
  private final UriPatternRouter<HttpStaticFileHandler> filesUriRouter;
  private final Set<HttpHandler> handlers;

  public HttpDispatcher(final UriRoutesBuilder routes) {
    this(newHttpMessageDispatcher(), routes);
  }

  public HttpDispatcher(final MessageDispatcher messageDispatcher, final UriRoutesBuilder routes) {
    this.messageDispatcher = messageDispatcher;
    this.staticUriRouter = new UriStaticRouter<>(routes.getStaticUri(), this::buildUriHandler);
    this.variableUriRouter = new UriVariableRouter<>(routes.getVariableUri(), this::buildUriHandler);
    this.patternUriRouter = new UriPatternRouter<>(routes.getPatternUri(), this::buildUriHandler);
    this.filesUriRouter = new UriPatternRouter<>(routes.getStaticFilesUri(), this::buildStaticFileUriHandler);
    this.handlers = routes.getHandlers();
  }

  private MethodInvoker buildUriHandler(final UriRoute route) {
    return messageDispatcher.newMethodInvoker(route.getHandler(), route.getMethod());
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

  public void close() {
    for (final HttpHandler handler: this.handlers) {
      handler.destroy();
    }
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
      final DnacoMetadataMap respHeaders = new DnacoMetadataMap();
      respHeaders.set(DnacoMessageUtil.METADATA_FOR_HTTP_STATUS, HttpResponseStatus.NO_CONTENT.code());
      return new DnacoMessage(packetId, respHeaders, Unpooled.EMPTY_BUFFER);
    }

    if (result instanceof final DnacoMessage message) {
      return message;
    }

    try {
      final ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer();
      ByteBufDataFormatUtil.addToBytes(JsonFormat.INSTANCE, buffer, result);
      final DnacoMetadataMap respHeaders = new DnacoMetadataMap();
      respHeaders.set(DnacoMessageUtil.METADATA_FOR_HTTP_STATUS, HttpResponseStatus.OK.code());
      return new DnacoMessage(packetId, respHeaders, buffer);
    } catch (final Throwable e) {
      return buildError(packetId, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private DnacoMessage buildError(final long packetId, final HttpResponseStatus status) {
    final DnacoMetadataMap metadata = new DnacoMetadataMap();
    metadata.set(DnacoMessageUtil.METADATA_FOR_HTTP_STATUS, status.code());
    return new DnacoMessage(packetId, metadata, Unpooled.EMPTY_BUFFER);
  }

  private static final String HEADER_KEY_CONTENT_TYPE = "content-type";
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
      return setMessage(new DnacoMessage(0, new DnacoMetadataMap(request.headers().entries()), request.content()));
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
    final int methodMask = HttpRouters.httpMethodMask(ctx.method());
    final String path = ctx.path();
    if (StringUtil.isEmpty(path)) return null;

    final MethodInvoker handler = getRequestHandler(ctx, methodMask, path);

    return handler;
  }

  private MethodInvoker getRequestHandler(final HttpCallContext ctx, final int methodMask, final String path) {
    // try lookup from static uris
    final MethodInvoker staticHandler = staticUriRouter.get(methodMask, path);
    if (staticHandler != null) {
      //Logger.trace("FOUND STATIC HANDLER for {}: {}", path, staticHandler);
      return staticHandler;
    }

    // try lookup from variable uris
    final UriVariableHandler<MethodInvoker> variableRes = variableUriRouter.get(methodMask, path);
    if (variableRes != null) {
      //Logger.trace("FOUND VARIABLE HANDLER for {}: {}", path, variableRes);
      ctx.setUriVariables(variableRes.getVariables());
      return variableRes.getHandler();
    }

    // try lookup from pattern uris
    final UriPatternHandler<MethodInvoker> patternRes = patternUriRouter.get(methodMask, path);
    if (patternRes != null) {
      //Logger.trace("FOUND PATTERN HANDLER for {}: {}", path, variableRes);
      ctx.setUriMatcher(patternRes.getMatcher());
      return patternRes.getHandler();
    }

    return null;
  }

  public static class HttpCallContext implements CallContext {
    private Map<String, String> uriVariables = Collections.emptyMap();
    private Matcher uriMatcher = null;

    private final ExecutionId executionId;
    private final Map<String, List<String>> queryParams;
    private final String method;
    private final String path;

    private DnacoMessage message = null;

    protected HttpCallContext(final HttpCallContext other) {
      this.executionId = other.executionId;
      this.uriVariables = other.uriVariables;
      this.uriMatcher = other.uriMatcher;
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

    // --------------------------------------------------
    //  uri variables
    // --------------------------------------------------
    public boolean hasUriVariables() {
      return MapUtil.isNotEmpty(uriVariables);
    }

    public String getUriVariable(final String key) {
      return uriVariables.get(key);
    }

    private void setUriVariables(final Map<String, String> variables) {
      this.uriVariables = variables;
    }

    // --------------------------------------------------
    //  url pattern variables
    // --------------------------------------------------
    public boolean hasUriMatcher() {
      return uriMatcher != null;
    }

    public Matcher getUriMatcher() {
      return uriMatcher;
    }

    private void setUriMatcher(final Matcher matcher) {
      this.uriMatcher = matcher;
    }
  }

  // ================================================================================
  //  PRIVATE HTTP Message Dispatcher
  // ================================================================================
  private static final String CONTENT_TYPE_FORM_URLENCODED = "application/x-www-form-urlencoded";
  private static final String CONTENT_TYPE_TEXT_XML = "text/xml";
  private static final String CONTENT_TYPE_APP_XML = "application/xml";
  private static final String CONTENT_TYPE_APP_CBOR = "application/cbor";
  private static final String CONTENT_TYPE_APP_JSON = "application/json";

  public static MessageDispatcher newHttpMessageDispatcher() {
    final MessageDispatcher dispatcher = new MessageDispatcher();
    // request parsers
    dispatcher.addParamAnnotationMapper(UriVariable.class, HttpUriVariableParamParser::new);
    dispatcher.addParamAnnotationMapper(HeaderValue.class, HttpHeaderParamParser::new);
    dispatcher.addParamAnnotationMapper(QueryParam.class, HttpQueryParamParser::new);
    // body parsers
    dispatcher.addParamAnnotationMapper(JsonBody.class, JsonFormatParamParser::new);
    dispatcher.addParamAnnotationMapper(CborBody.class, CborFormatParamParser::new);
    dispatcher.addParamAnnotationMapper(XmlBody.class, XmlFormatParamParser::new);
    dispatcher.addParamAnnotationMapper(FormEncodedBody.class, FormEncodedParamParser::new);
    dispatcher.addParamDefaultMapper(HttpDefaultParamParser::new);
    // ...
    dispatcher.addParamTypeMapper(ResultStream.class, ResultStreamParamParser::new);
    return dispatcher;
  }

  private static Object convertValue(final Class<?> type, List<String> values, final String defaultValue) {
    if (ListUtil.isEmpty(values)) {
      if (StringUtil.isEmpty(defaultValue)) return null;
      values = Collections.singletonList(defaultValue);
    }

    if (type.isArray()) {
      return JsonUtil.fromJson(JsonUtil.toJson(values), type);
    }
    return convertValue(type, values.get(0));
  }

  private static Object convertValue(final Class<?> type, final String value) {
    // gson Strings should be quoted.
    // TODO: Handle String[]
    if (String.class.isAssignableFrom(type) || type.isEnum()) {
      return JsonUtil.fromJson("\"" + value + "\"", type);
    }
    return JsonUtil.fromJson(value, type);
  }

  private static class HttpUriVariableParamParser implements ParamParser {
    private final Class<?> type;
    private final String name;

    private HttpUriVariableParamParser(final Parameter param, final Annotation annotation) {
      final UriVariable uriVariable = (UriVariable)annotation;
      this.name = StringUtil.defaultIfEmpty(uriVariable.value(), uriVariable.name());
      this.type = param.getType();
    }

    @Override
    public Object parse(final CallContext rawContext, final Object message) throws Exception {
      return convertValue(type, getUriVariable((HttpCallContext)rawContext, name));
    }

    private static String getUriVariable(final HttpCallContext ctx, final String name) {
      final String uriVar = ctx.getUriVariable(name);
      if (uriVar != null) return uriVar;

      final Matcher matcher = ctx.getUriMatcher();
      return matcher != null ? matcher.group(name) : null;
    }
  }

  private static class HttpHeaderParamParser implements ParamParser {
    private final Class<?> type;
    private final String name;

    private HttpHeaderParamParser(final Parameter param, final Annotation annotation) {
      final HeaderValue header = (HeaderValue)annotation;
      this.name = StringUtil.defaultIfEmpty(header.value(), header.name());
      this.type = param.getType();
    }

    @Override
    public Object parse(final CallContext rawContext, final Object message) throws Exception {
      final HttpCallContext context = ((HttpCallContext)rawContext);
      return convertValue(type, context.headerValueAsList(name), null);
    }
  }

  private static class HttpQueryParamParser implements ParamParser {
    private final String defaultValue;
    private final Class<?> type;
    private final String name;

    private HttpQueryParamParser(final Parameter param, final Annotation annotation) {
      final QueryParam queryParam = (QueryParam)annotation;
      this.name = StringUtil.defaultIfEmpty(queryParam.value(), queryParam.name());
      this.defaultValue = StringUtil.nullIfEmpty(queryParam.defaultValue());
      this.type = param.getType();
    }

    @Override
    public Object parse(final CallContext rawContext, final Object message) throws Exception {
      final HttpCallContext context = ((HttpCallContext)rawContext);
      final List<String> values = context.queryParamAsList(name);
      return convertValue(type, values, defaultValue);
    }
  }

  private static abstract class BodyParamParser implements ParamParser {
    private final Class<?> valueType;

    protected BodyParamParser(final Parameter param, final Annotation annotation) {
      this.valueType = param.getType();
    }

    protected Class<?> valueType() {
      return valueType;
    }

    @Override
    public Object parse(final CallContext context, final Object rawMessage) throws Exception {
      return parseBody((HttpCallContext)context, (DnacoMessage)rawMessage);
    }

    protected abstract Object parseBody(final HttpCallContext context, final DnacoMessage message) throws Exception;
  }

  private static abstract class BodyDataFormatParamParser extends BodyParamParser {
    private final DataFormat dataFormat;

    protected BodyDataFormatParamParser(final Parameter param, final Annotation annotation, final DataFormat dataFormat) {
      super(param, annotation);
      this.dataFormat = dataFormat;
    }

    protected Object parseBody(final HttpCallContext context, final DnacoMessage message) throws Exception {
      return ByteBufDataFormatUtil.fromBytes(dataFormat, message.data(), valueType());
    }
  }

  private static class XmlFormatParamParser extends BodyDataFormatParamParser {
    private XmlFormatParamParser(final Parameter param, final Annotation annotation) {
      super(param, annotation, XmlFormat.INSTANCE);
    }
  }

  private static class CborFormatParamParser extends BodyDataFormatParamParser {
    private CborFormatParamParser(final Parameter param, final Annotation annotation) {
      super(param, annotation, CborFormat.INSTANCE);
    }
  }

  private static class JsonFormatParamParser extends BodyDataFormatParamParser {
    private JsonFormatParamParser(final Parameter param, final Annotation annotation) {
      super(param, annotation, JsonFormat.INSTANCE);
    }
  }

  private static class FormEncodedParamParser extends BodyParamParser {
    private FormEncodedParamParser(final Parameter param, final Annotation annotation) {
      super(param, annotation);
    }

    @Override
    protected Object parseBody(final HttpCallContext context, final DnacoMessage message) throws Exception {
      return parseBody(message);
    }

    protected static Map<String, String> parseBody(final DnacoMessage message) throws IOException {
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

  private static class HttpDefaultParamParser extends BodyParamParser {
    private HttpDefaultParamParser(final Parameter param, final Annotation annotation) {
      super(param, annotation);
    }

    @Override
    protected Object parseBody(final HttpCallContext context, final DnacoMessage message) throws Exception {
      final String contentType = StringUtil.emptyIfNull(message.metadataMap().get(HEADER_KEY_CONTENT_TYPE));
      switch (contentType) {
        case CONTENT_TYPE_FORM_URLENCODED:
          return FormEncodedParamParser.parseBody(message);
        case CONTENT_TYPE_APP_XML, CONTENT_TYPE_TEXT_XML:
          return ByteBufDataFormatUtil.fromBytes(XmlFormat.INSTANCE, message.data(), valueType());
        case CONTENT_TYPE_APP_CBOR:
          return ByteBufDataFormatUtil.fromBytes(CborFormat.INSTANCE, message.data(), valueType());
      }
      // fallback to json
      return ByteBufDataFormatUtil.fromBytes(JsonFormat.INSTANCE, message.data(), valueType());
    }
  }

  private static class ResultStreamParamParser implements ParamParser {
    private ResultStreamParamParser(final Parameter param, final Annotation annotation) {
      // no-op
    }

    @Override
    public Object parse(final CallContext context, final Object message) throws Exception {
      return new PagedResultStream(((HttpCallContext)context).executionId());
    }
  }
}
