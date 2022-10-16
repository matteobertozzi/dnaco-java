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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.multipart.Attribute;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.handler.codec.http.multipart.InterfaceHttpData.HttpDataType;
import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.data.DataFormat;
import tech.dnaco.dispatcher.DispatchLaterException;
import tech.dnaco.dispatcher.MessageMapper;
import tech.dnaco.dispatcher.message.Message;
import tech.dnaco.dispatcher.message.MessageError;
import tech.dnaco.dispatcher.message.MessageHandler.FormEncodedBody;
import tech.dnaco.dispatcher.message.MessageMetadata;
import tech.dnaco.dispatcher.message.MessageUtil;
import tech.dnaco.dispatcher.message.UriDispatcher;
import tech.dnaco.dispatcher.message.UriMessage;
import tech.dnaco.dispatcher.message.UriRouters.StaticFileUriRoute;
import tech.dnaco.dispatcher.message.UriRouters.UriPatternHandler;
import tech.dnaco.dispatcher.message.UriRouters.UriPatternRouter;
import tech.dnaco.dispatcher.message.UriRouters.UriRoutesBuilder;
import tech.dnaco.logging.Logger;
import tech.dnaco.net.util.ByteBufDataFormatUtil;
import tech.dnaco.strings.StringUtil;

public class HttpDispatcher extends UriDispatcher {
  private final UriPatternRouter<HttpStaticFileHandler> filesUriRouter;

  public HttpDispatcher(final UriRoutesBuilder routes) {
    this(newHttpMessageMapper(), routes);
  }

  public HttpDispatcher(final MessageMapper messageDispatcher, final UriRoutesBuilder routes) {
    super(messageDispatcher, HttpMessageBuilder.INSTANCE, routes);
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
      final FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR, Unpooled.EMPTY_BUFFER);
      ctx.write(response);
    }
    return true;
  }

  public MessageTask prepare(final FullHttpRequest request) {
    return prepare(new HttpMessageRequest(request));
  }

  public void execute(final ChannelHandlerContext ctx, final MessageTask task) throws DispatchLaterException {
    final Message response = task.execute();
    if (response instanceof final HttpMessageResponse httpResponse) {
      ctx.write(httpResponse.rawResponse());
    } else {
      throw new IllegalArgumentException("unexpected message " + response.getClass() + ": " + response);
    }
  }

  public void sendErrorMessage(final ChannelHandlerContext ctx, final FullHttpRequest request, final MessageError error) {
    final Message response = newErrorMessage(request, error);
    if (response instanceof final HttpMessageResponse httpResponse) {
      ctx.write(httpResponse.rawResponse());
    } else {
      throw new IllegalArgumentException("unexpected message " + response.getClass() + ": " + response);
    }
  }

  public Message newErrorMessage(final FullHttpRequest request, final MessageError error) {
    return newErrorMessage(new HttpMessageMetadata(request.headers()), error);
  }

  private static final class HttpMessageBuilder implements MessageBuilder {
    private static final HttpMessageBuilder INSTANCE = new HttpMessageBuilder();

    private HttpMessageBuilder() {
      // no-op
    }

    @Override
    public Message newErrorMessage(final MessageMetadata reqMetadata, final MessageMetadata resultMetadata,
        final DataFormat format, final MessageError error) {
      final ByteBuf buffer = error.hasBody() ? ByteBufDataFormatUtil.asBytes(format, error) : Unpooled.EMPTY_BUFFER;
      final HttpResponseStatus status = HttpResponseStatus.valueOf(error.statusCode());
      return newMessage(reqMetadata, status, resultMetadata, format.contentType(), buffer);
    }

    @Override
    public Message newMessage(final MessageMetadata reqMetadata, final MessageMetadata resultMetadata,
        final DataFormat format, final Object result) {
      final ByteBuf buffer = ByteBufDataFormatUtil.asBytes(format, result);
      final int code = resultMetadata.getInt(MessageUtil.METADATA_FOR_HTTP_STATUS, buffer.readableBytes() > 0 ? 200 : 204);
      return newMessage(reqMetadata, HttpResponseStatus.valueOf(code), resultMetadata, format.contentType(), buffer);
    }

    @Override
    public Message newMessage(final MessageMetadata reqMetadata, final MessageMetadata resultMetadata, final byte[] result) {
      final int code = resultMetadata.getInt(MessageUtil.METADATA_FOR_HTTP_STATUS, BytesUtil.isEmpty(result) ? 204 : 200);
      return newMessage(reqMetadata, HttpResponseStatus.valueOf(code), resultMetadata, null, Unpooled.wrappedBuffer(result));
    }

    @Override
    public Message newEmptyMessage(final MessageMetadata reqMetadata, final MessageMetadata resultMetadata) {
      final int code = resultMetadata.getInt(MessageUtil.METADATA_FOR_HTTP_STATUS, 204);
      return newMessage(reqMetadata, HttpResponseStatus.valueOf(code), resultMetadata, null, Unpooled.EMPTY_BUFFER);
    }

    private Message newMessage(final MessageMetadata reqMetadata, final HttpResponseStatus status,
        final MessageMetadata resultMetadata, final String contentType, final ByteBuf body) {
      final int contentLength = body.readableBytes();

      if (status == null) {
        Logger.error(new Exception("missing status"), "status missing");
      }

      final FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, body, false);
      final HttpHeaders headers = response.headers();
      addHeaders(headers, reqMetadata, contentType, contentLength);
      resultMetadata.forEach((k, v) -> {
        if (k.charAt(0) != ':') {
          headers.add(k, v);
        }
      });
      //System.out.println("NEW MESSAGE STATUS " + status + " HEADERS: " + headers);
      return new HttpMessageResponse(response);
    }

    private static final boolean KEEP_ALIVE_SUPPORTED = true;
    private static void addHeaders(final HttpHeaders headers, final MessageMetadata reqMetadata, final String contentType, final int contentLength) {
      // default response headers
      headers.add(HttpHeaderNames.DATE, new Date());
      headers.setInt(HttpHeaderNames.CONTENT_LENGTH, contentLength);
      if (contentType != null) {
        headers.set(HttpHeaderNames.CONTENT_TYPE, contentType);
      }

      // keep alive
      final String keepAlive = reqMetadata.getString("connection", null);
      if (KEEP_ALIVE_SUPPORTED && StringUtil.equalsIgnoreCase(keepAlive, "keep-alive")) {
        headers.set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
      }
    }
  }

  // ================================================================================
  //  PRIVATE HTTP Message Dispatcher
  // ================================================================================
  public static MessageMapper newHttpMessageMapper() {
    final MessageMapper mapper = newMessageMapper();
    // body parsers
    mapper.addParamAnnotationMapper(FormEncodedBody.class, FormEncodedParamParser::new);
    return mapper;
  }

  private static class FormEncodedParamParser extends BodyParamParser {
    private FormEncodedParamParser(final Parameter param, final Annotation annotation) {
      super(param, annotation);
    }

    @Override
    protected Object parseBody(final MessageCallContext context, final UriMessage message) throws Exception {
      return parseBody(message);
    }

    protected static Map<String, String> parseBody(final Message message) throws IOException {
      if (message instanceof final HttpMessageRequest httpRequest) {
        final FullHttpRequest request = httpRequest.rawRequest();
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
      return Collections.emptyMap();
    }
  }
}
