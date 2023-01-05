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
import java.io.File;
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
import tech.dnaco.data.DataFormat.DataFormatException;
import tech.dnaco.data.JsonFormat;
import tech.dnaco.data.XmlFormat;
import tech.dnaco.data.json.JsonUtil;
import tech.dnaco.dispatcher.CallContext;
import tech.dnaco.dispatcher.DispatchLaterException;
import tech.dnaco.dispatcher.Invokable;
import tech.dnaco.dispatcher.MessageMapper;
import tech.dnaco.dispatcher.MethodInvoker;
import tech.dnaco.dispatcher.ParamParser;
import tech.dnaco.dispatcher.message.MessageHandler.CborBody;
import tech.dnaco.dispatcher.message.MessageHandler.HeaderValue;
import tech.dnaco.dispatcher.message.MessageHandler.JsonBody;
import tech.dnaco.dispatcher.message.MessageHandler.MetaParam;
import tech.dnaco.dispatcher.message.MessageHandler.QueryParam;
import tech.dnaco.dispatcher.message.MessageHandler.UriMethod;
import tech.dnaco.dispatcher.message.MessageHandler.UriPattern;
import tech.dnaco.dispatcher.message.MessageHandler.UriVariable;
import tech.dnaco.dispatcher.message.MessageHandler.XmlBody;
import tech.dnaco.dispatcher.message.MessageUtil.EmptyMessage;
import tech.dnaco.dispatcher.message.MessageUtil.EmptyMetadata;
import tech.dnaco.dispatcher.message.MessageUtil.RawMessage;
import tech.dnaco.dispatcher.message.MessageUtil.TypedMessage;
import tech.dnaco.dispatcher.message.UriRouters.UriPatternHandler;
import tech.dnaco.dispatcher.message.UriRouters.UriPatternRouter;
import tech.dnaco.dispatcher.message.UriRouters.UriRoute;
import tech.dnaco.dispatcher.message.UriRouters.UriRoutesBuilder;
import tech.dnaco.dispatcher.message.UriRouters.UriStaticRouter;
import tech.dnaco.dispatcher.message.UriRouters.UriVariableHandler;
import tech.dnaco.dispatcher.message.UriRouters.UriVariableRouter;
import tech.dnaco.logging.Logger;
import tech.dnaco.strings.StringConverter;
import tech.dnaco.strings.StringUtil;

public abstract class UriDispatcher implements Closeable {
  public interface MessageBuilder {
    Message newErrorMessage(MessageMetadata request, MessageMetadata resultMetadata, DataFormat format, MessageError error);
    Message newMessage(MessageMetadata request, MessageMetadata resultMetadata, DataFormat format, Object result);
    Message newMessage(MessageMetadata request, MessageMetadata resultMetadata, byte[] result);
    Message newEmptyMessage(MessageMetadata request, MessageMetadata resultMetadata);
    Message newFileStream(MessageMetadata requestMetadata, MessageMetadata resultMetadata, File file);
  }

  private final MessageMapper messageDispatcher;
  private final MessageBuilder messageBuilder;

  private final UriStaticRouter<MethodInvoker> staticUriRouter;
  private final UriVariableRouter<MethodInvoker> variableUriRouter;
  private final UriPatternRouter<MethodInvoker> patternUriRouter;
  private final Set<MessageHandler> handlers;

  public UriDispatcher(final MessageBuilder msgBuilder, final UriRoutesBuilder routes) {
    this(newMessageMapper(), msgBuilder, routes);
  }

  public UriDispatcher(final MessageMapper msgDispatcher, final MessageBuilder msgBuilder, final UriRoutesBuilder routes) {
    this.messageDispatcher = msgDispatcher;
    this.messageBuilder = msgBuilder;
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

  public Message rawExecute(final UriMessage message) throws DispatchLaterException {
    final MessageCallContext ctx = new MessageCallContext();
    final MethodInvoker handler = getRequestHandler(ctx, message.method(), message.path());
    if (handler == null) {
      return newErrorMessage(message.metadata(), MessageError.notFound());
    }
    return execute(message.metadata(), () -> handler.invoke(ctx, message),
      handler.hasAsyncResult(), handler.hasVoidResult());
  }

  public MessageTask prepare(final UriMessage message) {
    final MessageCallContext ctx = new MessageCallContext();
    final MethodInvoker methodInvoker = getRequestHandler(ctx, message.method(), message.path());
    return methodInvoker != null ? new MessageTask(this, methodInvoker, ctx, message, System.nanoTime()) : null;
  }

  public Message execute(final MessageTask task) throws DispatchLaterException {
    return execute(task.metadata(), task::invoke, task.hasAsyncResult(), task.hasVoidResult());
  }

  public Message execute(final MessageMetadata requestMetadata, final Invokable invokable,
      final boolean hasAsyncResult, final boolean hasVoidResult) throws DispatchLaterException {
    try {
      return executeInternal(requestMetadata, invokable, hasAsyncResult, hasVoidResult);
    } catch (final DispatchLaterException e) {
      throw e;
    } catch (final Throwable e) {
      Logger.error(e, "internal server error during execution");
      return newErrorMessage(requestMetadata, MessageError.internalServerError());
    }
  }

  private Message executeInternal(final MessageMetadata requestMetadata, final Invokable invokable,
      final boolean hasAsyncResult, final boolean hasVoidResult) throws DispatchLaterException {
    try {
      final Object result = invokable.invoke();
      return convertResult(hasAsyncResult, hasVoidResult, requestMetadata, EmptyMetadata.INSTANCE, result);
    } catch (final DispatchLaterException e) {
      throw e;
    } catch (final MessageException e) {
      if (e.shouldLogTrace()) {
        Logger.error(e, "message exception: {}", e.getMessage());
      } else {
        Logger.error("message exception: {}", e.getMessage());
      }
      return newErrorMessage(requestMetadata, e.getMessageError());
    } catch (final DataFormatException e) {
      Logger.error(e, "data format exception");
      return newErrorMessage(requestMetadata, MessageError.newBadRequestError(e.getMessage()));
    } catch (final Throwable e) {
      final MessageError error =  messageDispatcher.mapException(e);
      if (error != null) {
        Logger.error(e, "message exception");
        return newErrorMessage(requestMetadata, error);
      }
      Logger.error(e, "internal server error");
      return newErrorMessage(requestMetadata, MessageError.internalServerError());
    }
  }

  public Message convertResult(final boolean hasAsyncResult, final boolean hasVoidResult,
      final MessageMetadata requestMetadata, final MessageMetadata defaultResultMetadata, final Object result) {
    if (result == null) {
      if (hasAsyncResult) {
        // another thread will send the response
        return null;
      } else if (hasVoidResult) {
        return messageBuilder.newEmptyMessage(requestMetadata, defaultResultMetadata);
      }
    }

    if (result instanceof final Message messageResult) {
      return convertMessageResult(hasAsyncResult, hasVoidResult, requestMetadata, defaultResultMetadata, messageResult);
    }

    if (result instanceof final byte[] bytesResult) {
      return messageBuilder.newMessage(requestMetadata, defaultResultMetadata, bytesResult);
    }

    final Message messageResult = this.messageDispatcher.mapTypedResultToMessage(result);
    if (messageResult != null) {
      return convertMessageResult(hasAsyncResult, hasVoidResult, requestMetadata, defaultResultMetadata, messageResult);
    }

    final DataFormat format = MessageUtil.parseAcceptFormat(requestMetadata);
    return messageBuilder.newMessage(requestMetadata, defaultResultMetadata, format, result);
  }

  private Message convertMessageResult(final boolean hasAsyncResult, final boolean hasVoidResult,
      final MessageMetadata requestMetadata, final MessageMetadata defaultResultMetadata, final Message result) {
    if (result instanceof final RawMessage rawResult) {
      return messageBuilder.newMessage(requestMetadata, rawResult.metadata(), rawResult.content());
    } else if (result instanceof final TypedMessage<?> objResult) {
      final DataFormat format = MessageUtil.parseAcceptFormat(requestMetadata);
      return messageBuilder.newMessage(requestMetadata, objResult.metadata(), format, objResult.content());
    } else if (result instanceof final EmptyMessage emptyResult) {
      return messageBuilder.newEmptyMessage(requestMetadata, emptyResult.metadata());
    } else if (result instanceof final MessageFile fileResult) {
      return messageBuilder.newFileStream(requestMetadata, fileResult.metadata(), fileResult.file());
    }
    throw new IllegalArgumentException("unsupported message type: " + result.getClass().getName());
  }

  public Message newErrorMessage(final MessageMetadata request, final MessageError error) {
    final DataFormat format = MessageUtil.parseAcceptFormat(request);
    return messageBuilder.newErrorMessage(request, EmptyMetadata.INSTANCE, format, error);
  }

  // ================================================================================
  //  PROTECTED call context & task
  // ================================================================================
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


  public static class MessageTask {
    private final UriDispatcher dispatcher;
    private final MethodInvoker methodInvoker;
    private final MessageCallContext ctx;
    private final UriMessage message;
    private final long ctime;

    protected MessageTask(final MessageTask other) {
      this(other.dispatcher, other.methodInvoker, other.ctx, other.message, other.ctime);
    }

    private MessageTask(final UriDispatcher dispatcher, final MethodInvoker methodInvoker,
        final MessageCallContext ctx, final UriMessage message, final long creationTime) {
      this.ctime = creationTime;
      this.dispatcher = dispatcher;
      this.methodInvoker = methodInvoker;
      this.ctx = ctx;
      this.message = message;
    }

    public long creationTime() {
      return ctime;
    }

    protected boolean hasAsyncResult() {
      return methodInvoker.hasAsyncResult();
    }

    protected boolean hasVoidResult() {
      return methodInvoker.hasVoidResult();
    }

    protected Object invoke() throws Throwable {
      return methodInvoker.invoke(ctx, message);
    }

    public UriMessage message() {
      return message;
    }

    public MessageMetadata metadata() {
      return message.metadata();
    }

    public boolean hasAnnotation(final Class<? extends Annotation> annotationType) {
      return methodInvoker.hasAnnotation(annotationType);
    }

    public Message execute() throws DispatchLaterException {
      return dispatcher.execute(this);
    }
  }


  // ================================================================================
  //  PROTECTED mapping and execute
  // ================================================================================
  protected MethodInvoker getRequestHandler(final MessageCallContext ctx, final UriMethod method, final String path) {
    return getRequestHandler(ctx, UriRouters.uriMethodMask(method), path);
  }

  protected MethodInvoker getRequestHandler(final MessageCallContext ctx, final int methodMask, final String path) {
    if (StringUtil.isEmpty(path)) return null;

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
  //  PRIVATE Message Dispatcher
  // ================================================================================
  public static MessageMapper newMessageMapper() {
    final MessageMapper mapper = new MessageMapper();
    // request parsers
    mapper.addParamAnnotationMapper(UriVariable.class, UriVariableParamParser::new);
    mapper.addParamAnnotationMapper(UriPattern.class, UriPatternParamParser::new);
    // metadata parsers
    mapper.addParamAnnotationMapper(HeaderValue.class, HeaderParamParser::new);
    mapper.addParamAnnotationMapper(QueryParam.class, QueryParamParser::new);
    mapper.addParamAnnotationMapper(MetaParam.class, MetaParamParser::new);
    // body parsers
    mapper.addParamAnnotationMapper(JsonBody.class, JsonFormatParamParser::new);
    mapper.addParamAnnotationMapper(CborBody.class, CborFormatParamParser::new);
    mapper.addParamAnnotationMapper(XmlBody.class, XmlFormatParamParser::new);
    mapper.addParamDefaultMapper(DefaultParamParser::new);
    // ...
    //mapper.addParamTypeMapper(ResultStream.class, ResultStreamParamParser::new);
    return mapper;
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
    if (type == String.class) {
      return value;
    } else if (type == boolean.class) {
      return StringConverter.toBoolean(value, false);
    } else if (type == int.class) {
      return StringConverter.toInt(value, 0);
    } else if (type == long.class) {
      return StringConverter.toLong(value, 0);
    } else if (type == float.class) {
      return StringConverter.toFloat(value, 0);
    } else if (type == double.class) {
      return StringConverter.toDouble(value, 0);
    }

    // gson Strings should be quoted.
    // TODO: Handle String[]
    if (String.class.isAssignableFrom(type) || type.isEnum()) {
      final String json = "\"" + value.replace("\"", "\\\"") + "\"";
      return JsonUtil.fromJson(json, type);
    }
    return JsonUtil.fromJson(value, type);
  }

  // ================================================================================
  //  PRIVATE Request Param Parsers
  // ================================================================================
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

  // ================================================================================
  //  PRIVATE Metadata Param Parsers
  // ================================================================================
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
      final Message message = (Message)rawMessage;
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
      final UriMessage message = (UriMessage)rawMessage;
      final List<String> values = message.queryParamAsList(name);
      return convertValue(type, values, defaultValue);
    }
  }

  private static final class MetaParamParser implements ParamParser {
    private final String defaultValue;
    private final Class<?> type;
    private final String queryParam;
    private final String headerKey;

    private MetaParamParser(final Parameter param, final Annotation annotation) {
      final MetaParam metaParam = (MetaParam)annotation;
      this.queryParam = metaParam.query();
      this.headerKey = metaParam.header();
      this.defaultValue = StringUtil.nullIfEmpty(metaParam.defaultValue());
      this.type = param.getType();
    }

    @Override
    public Object parse(final CallContext rawContext, final Object rawMessage) throws Exception {
      final UriMessage message = (UriMessage)rawMessage;
      List<String> values = message.metadataValueAsList(headerKey);
      if (ListUtil.isEmpty(values)) values = message.queryParamAsList(queryParam);
      return convertValue(type, values, defaultValue);
    }
  }

  // ================================================================================
  //  PRIVATE Body Param Parsers
  // ================================================================================
  protected static abstract class BodyParamParser implements ParamParser {
    private final Class<?> valueType;

    protected BodyParamParser(final Parameter param, final Annotation annotation) {
      this.valueType = param.getType();
    }

    protected Class<?> valueType() {
      return valueType;
    }

    @Override
    public Object parse(final CallContext context, final Object rawMessage) throws Exception {
      return parseBody((MessageCallContext)context, (UriMessage)rawMessage);
    }

    protected abstract Object parseBody(final MessageCallContext context, final UriMessage message) throws Exception;
  }

  private static abstract class BodyDataFormatParamParser extends BodyParamParser {
    private final DataFormat dataFormat;

    protected BodyDataFormatParamParser(final Parameter param, final Annotation annotation, final DataFormat dataFormat) {
      super(param, annotation);
      this.dataFormat = dataFormat;
    }

    protected Object parseBody(final MessageCallContext context, final UriMessage message) throws Exception {
      return message.convertContent(dataFormat, valueType());
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
    protected Object parseBody(final MessageCallContext context, final UriMessage message) throws Exception {
      Logger.debug("convert body type {} {}", valueType(), message);
      // if the method wants the "raw message" return it directly
      if (valueType().isAssignableFrom(UriMessage.class)) {
        return message;
      }

      if (valueType() == byte[].class) {
        return message.convertContentToBytes();
      }

      // perform a format conversion to the desired type
      final String contentType = message.getMetadata(MessageUtil.METADATA_CONTENT_TYPE, "");
      switch (contentType) {
        case MessageUtil.CONTENT_TYPE_APP_XML, MessageUtil.CONTENT_TYPE_TEXT_XML:
          return message.convertContent(XmlFormat.INSTANCE, valueType());
        case MessageUtil.CONTENT_TYPE_APP_CBOR:
          return message.convertContent(CborFormat.INSTANCE, valueType());
      }
      // fallback to json
      return message.convertContent(JsonFormat.INSTANCE, valueType());
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
