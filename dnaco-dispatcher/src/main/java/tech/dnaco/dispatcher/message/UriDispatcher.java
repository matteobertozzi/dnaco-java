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
package tech.dnaco.dispatcher.message;

import java.io.Closeable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Parameter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

import tech.dnaco.collections.lists.ListUtil;
import tech.dnaco.collections.maps.MapUtil;
import tech.dnaco.data.CborFormat;
import tech.dnaco.data.DataFormat;
import tech.dnaco.data.JsonFormat;
import tech.dnaco.data.XmlFormat;
import tech.dnaco.data.json.JsonUtil;
import tech.dnaco.dispatcher.CallContext;
import tech.dnaco.dispatcher.MessageDispatcher;
import tech.dnaco.dispatcher.MethodInvoker;
import tech.dnaco.dispatcher.ParamParser;
import tech.dnaco.dispatcher.message.MessageHandler.CborBody;
import tech.dnaco.dispatcher.message.MessageHandler.HeaderValue;
import tech.dnaco.dispatcher.message.MessageHandler.JsonBody;
import tech.dnaco.dispatcher.message.MessageHandler.MessageData;
import tech.dnaco.dispatcher.message.MessageHandler.QueryParam;
import tech.dnaco.dispatcher.message.MessageHandler.UriPattern;
import tech.dnaco.dispatcher.message.MessageHandler.UriVariable;
import tech.dnaco.dispatcher.message.MessageHandler.XmlBody;
import tech.dnaco.dispatcher.message.UriRouters.UriPatternHandler;
import tech.dnaco.dispatcher.message.UriRouters.UriPatternRouter;
import tech.dnaco.dispatcher.message.UriRouters.UriRoute;
import tech.dnaco.dispatcher.message.UriRouters.UriRoutesBuilder;
import tech.dnaco.dispatcher.message.UriRouters.UriStaticRouter;
import tech.dnaco.dispatcher.message.UriRouters.UriVariableHandler;
import tech.dnaco.dispatcher.message.UriRouters.UriVariableRouter;
import tech.dnaco.strings.StringUtil;

public class UriDispatcher implements Closeable {
  private final MessageDispatcher messageDispatcher;

  private final UriStaticRouter<MethodInvoker> staticUriRouter;
  private final UriVariableRouter<MethodInvoker> variableUriRouter;
  private final UriPatternRouter<MethodInvoker> patternUriRouter;
  private final Set<MessageHandler> handlers;

  public UriDispatcher(final UriRoutesBuilder routes) {
    this(newMessageDispatcher(), routes);
  }

  public UriDispatcher(final MessageDispatcher messageDispatcher, final UriRoutesBuilder routes) {
    this.messageDispatcher = messageDispatcher;
    this.staticUriRouter = new UriStaticRouter<>(routes.getStaticUri(), this::buildUriHandler);
    this.variableUriRouter = new UriVariableRouter<>(routes.getVariableUri(), this::buildUriHandler);
    this.patternUriRouter = new UriPatternRouter<>(routes.getPatternUri(), this::buildUriHandler);
    this.handlers = routes.getHandlers();
  }

  private MethodInvoker buildUriHandler(final UriRoute route) {
    return messageDispatcher.newMethodInvoker(route.getHandler(), route.getMethod());
  }

  @Override
  public void close() {
    for (final MessageHandler handler: this.handlers) {
      handler.destroy();
    }
  }

  public void exec(final MessageData message) throws Throwable {
    final int methodMask = UriRouters.uriMethodMask(message.method());
    final String path = message.path();
    if (StringUtil.isEmpty(path)) return;

    final MessageCallContext ctx = new MessageCallContext();
    final MethodInvoker handler = getRequestHandler(ctx, methodMask, path);
    if (handler == null) return;

    handler.invoke(ctx, message);
  }

  protected static class MessageCallContext implements CallContext {
    private Map<String, String> uriVariables = Collections.emptyMap();
    private Matcher uriMatcher = null;

    protected MessageCallContext() {
      // no-op
    }

    protected MessageCallContext(final MessageCallContext other) {
      this.uriVariables = other.uriVariables;
      this.uriMatcher = other.uriMatcher;
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
  // PROTECTED mapping and execute
  // ================================================================================
  protected MethodInvoker getRequestHandler(final MessageCallContext ctx, final int methodMask, final String path) {
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

  // ================================================================================
  //  PRIVATE HTTP Message Dispatcher
  // ================================================================================
  private static final String CONTENT_TYPE_FORM_URLENCODED = "application/x-www-form-urlencoded";
  private static final String CONTENT_TYPE_TEXT_XML = "text/xml";
  private static final String CONTENT_TYPE_APP_XML = "application/xml";
  private static final String CONTENT_TYPE_APP_CBOR = "application/cbor";
  private static final String CONTENT_TYPE_APP_JSON = "application/json";

  private static final String HEADER_KEY_CONTENT_TYPE = "content-type";
  private static final String HEADER_KEY_ACCEPT = "accept";

  public static MessageDispatcher newMessageDispatcher() {
    final MessageDispatcher dispatcher = new MessageDispatcher();
    // request parsers
    dispatcher.addParamAnnotationMapper(UriVariable.class, UriVariableParamParser::new);
    dispatcher.addParamAnnotationMapper(UriPattern.class, UriPatternParamParser::new);
    dispatcher.addParamAnnotationMapper(HeaderValue.class, HeaderParamParser::new);
    dispatcher.addParamAnnotationMapper(QueryParam.class, QueryParamParser::new);
    // body parsers
    dispatcher.addParamAnnotationMapper(JsonBody.class, JsonFormatParamParser::new);
    dispatcher.addParamAnnotationMapper(CborBody.class, CborFormatParamParser::new);
    dispatcher.addParamAnnotationMapper(XmlBody.class, XmlFormatParamParser::new);
    dispatcher.addParamDefaultMapper(DefaultParamParser::new);
    // ...
    //dispatcher.addParamTypeMapper(ResultStream.class, ResultStreamParamParser::new);
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
      final String json = "\"" + value.replace("\"", "\\\"") + "\"";
      return JsonUtil.fromJson(json, type);
    }
    return JsonUtil.fromJson(value, type);
  }

  private static final class UriPatternParamParser implements ParamParser {
    private final int groupId;

    private UriPatternParamParser(final Parameter param, final Annotation annotation) {
      final UriPattern uriPattern = (UriPattern)annotation;
      this.groupId = uriPattern.value();
    }

    @Override
    public Object parse(final CallContext rawContext, final Object message) throws Exception {
      final Matcher matcher = ((MessageCallContext)rawContext).getUriMatcher();
      return (groupId >= 0) ? matcher.group(groupId) : matcher;
    }
  }

  private static final class UriVariableParamParser implements ParamParser {
    private final Class<?> type;
    private final String name;

    private UriVariableParamParser(final Parameter param, final Annotation annotation) {
      final UriVariable uriVariable = (UriVariable)annotation;
      this.name = uriVariable.value();
      this.type = param.getType();
    }

    @Override
    public Object parse(final CallContext rawContext, final Object message) throws Exception {
      return convertValue(type, getUriVariable((MessageCallContext)rawContext, name));
    }

    private static String getUriVariable(final MessageCallContext ctx, final String name) {
      final String uriVar = ctx.getUriVariable(name);
      if (uriVar != null) return uriVar;

      final Matcher matcher = ctx.getUriMatcher();
      return matcher != null ? matcher.group(name) : null;
    }
  }

  private static final class HeaderParamParser implements ParamParser {
    private final String defaultValue;
    private final Class<?> type;
    private final String name;

    private HeaderParamParser(final Parameter param, final Annotation annotation) {
      final HeaderValue header = (HeaderValue)annotation;
      this.name = StringUtil.defaultIfEmpty(header.value(), header.name());
      this.defaultValue = StringUtil.nullIfEmpty(header.defaultValue());
      this.type = param.getType();
    }

    @Override
    public Object parse(final CallContext rawContext, final Object rawMessage) throws Exception {
      final MessageData message = (MessageData)rawMessage;
      return convertValue(type, message.metadataValueAsList(name), defaultValue);
    }
  }

  private static final class QueryParamParser implements ParamParser {
    private final String defaultValue;
    private final Class<?> type;
    private final String name;

    private QueryParamParser(final Parameter param, final Annotation annotation) {
      final QueryParam queryParam = (QueryParam)annotation;
      this.name = StringUtil.defaultIfEmpty(queryParam.value(), queryParam.name());
      this.defaultValue = StringUtil.nullIfEmpty(queryParam.defaultValue());
      this.type = param.getType();
    }

    @Override
    public Object parse(final CallContext rawContext, final Object rawMessage) throws Exception {
      final MessageData message = (MessageData)rawMessage;
      final List<String> values = message.queryParamAsList(name);
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
      return parseBody((MessageCallContext)context, (MessageData)rawMessage);
    }

    protected abstract Object parseBody(final MessageCallContext context, final MessageData message) throws Exception;
  }

  private static abstract class BodyDataFormatParamParser extends BodyParamParser {
    private final DataFormat dataFormat;

    protected BodyDataFormatParamParser(final Parameter param, final Annotation annotation, final DataFormat dataFormat) {
      super(param, annotation);
      this.dataFormat = dataFormat;
    }

    protected Object parseBody(final MessageCallContext context, final MessageData message) throws Exception {
      return message.convertBody(dataFormat, valueType());
    }
  }

  private static final class XmlFormatParamParser extends BodyDataFormatParamParser {
    private XmlFormatParamParser(final Parameter param, final Annotation annotation) {
      super(param, annotation, XmlFormat.INSTANCE);
    }
  }

  private static final class CborFormatParamParser extends BodyDataFormatParamParser {
    private CborFormatParamParser(final Parameter param, final Annotation annotation) {
      super(param, annotation, CborFormat.INSTANCE);
    }
  }

  private static final class JsonFormatParamParser extends BodyDataFormatParamParser {
    private JsonFormatParamParser(final Parameter param, final Annotation annotation) {
      super(param, annotation, JsonFormat.INSTANCE);
    }
  }

  private static final class DefaultParamParser extends BodyParamParser {
    private DefaultParamParser(final Parameter param, final Annotation annotation) {
      super(param, annotation);
    }

    @Override
    protected Object parseBody(final MessageCallContext context, final MessageData message) throws Exception {
      final String contentType = message.getMetadata(HEADER_KEY_CONTENT_TYPE, "");
      switch (contentType) {
        case CONTENT_TYPE_APP_XML, CONTENT_TYPE_TEXT_XML:
          return message.convertBody(XmlFormat.INSTANCE, valueType());
        case CONTENT_TYPE_APP_CBOR:
          return message.convertBody(CborFormat.INSTANCE, valueType());
      }
      // fallback to json
      return message.convertBody(JsonFormat.INSTANCE, valueType());
    }
  }
/*
  private static class ResultStreamParamParser implements ParamParser {
    private ResultStreamParamParser(final Parameter param, final Annotation annotation) {
      // no-op
    }

    @Override
    public Object parse(final CallContext context, final Object message) throws Exception {
      return new PagedResultStream(((MessageCallContext)context).executionId());
    }
  }
*/
}
