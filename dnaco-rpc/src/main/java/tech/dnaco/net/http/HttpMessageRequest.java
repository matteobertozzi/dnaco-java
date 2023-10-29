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

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import tech.dnaco.collections.lists.ListUtil;
import tech.dnaco.collections.sets.SetUtil;
import tech.dnaco.data.DataFormat;
import tech.dnaco.dispatcher.message.Message;
import tech.dnaco.dispatcher.message.MessageError;
import tech.dnaco.dispatcher.message.MessageException;
import tech.dnaco.dispatcher.message.MessageHandler.UriMethod;
import tech.dnaco.dispatcher.message.MessageMetadata;
import tech.dnaco.dispatcher.message.UriMessage;
import tech.dnaco.logging.Logger;
import tech.dnaco.net.util.ByteBufDataFormatUtil;
import tech.dnaco.net.util.UriUtil;
import tech.dnaco.strings.StringUtil;

public class HttpMessageRequest implements UriMessage {
  private final HttpMessageQueryParams queryParams;
  private final HttpMessageMetadata metadata;
  private final FullHttpRequest request;
  private final UriMethod method;
  private final String path;
  private final long timestamp;

  public HttpMessageRequest(final FullHttpRequest request) {
    this.timestamp = System.nanoTime();

    this.metadata = new HttpMessageMetadata(request.headers());
    this.request = request;
    this.method = UriMethod.valueOf(request.method().name());

    final QueryStringDecoder queryDecoder = new QueryStringDecoder(request.uri());
    this.path = queryDecoder.path();
    this.queryParams = new HttpMessageQueryParams(queryDecoder.parameters());
  }

  @Override
  public Message retain() {
    request.content().retain();
    return this;
  }

  @Override
  public Message release() {
    request.content().release();
    return this;
  }

  @Override
  public long timestampNs() {
    return timestamp;
  }

  protected FullHttpRequest rawRequest() {
    return request;
  }

  @Override
  public int estimateSize() {
    return 4 + method.name().length()
         + 4 + path.length()
         + metadata.estimateSpace()
         + request.content().readableBytes();
  }

  @Override
  public MessageMetadata queryParams() {
    return queryParams;
  }

  @Override
  public MessageMetadata metadata() {
    return metadata;
  }

  @Override
  public int contentLength() {
    return request.content().readableBytes();
  }

  @Override
  public long writeContentToStream(final OutputStream stream) throws IOException {
    return ByteBufDataFormatUtil.transferTo(request.content(), stream);
  }

  @Override
  public long writeContentToStream(final DataOutput stream) throws IOException {
    return ByteBufDataFormatUtil.transferTo(request.content(), stream);
  }

  @Override
  public <T> T convertContent(final DataFormat format, final Class<T> classOfT) {
    return ByteBufDataFormatUtil.fromBytes(format, request.content(), classOfT);
  }

  @Override
  public UriMethod method() {
    return method;
  }

  @Override
  public String path() {
    return path;
  }

  public Map<String, String> decodeCookies() {
    final String cookieString = request.headers().get(HttpHeaderNames.COOKIE);
    if (StringUtil.isEmpty(cookieString)) return Collections.emptyMap();

    final Set<Cookie> cookies = ServerCookieDecoder.LAX.decode(cookieString);
    if (SetUtil.isEmpty(cookies)) return Collections.emptyMap();

    final Map<String, String> cookieMap = new HashMap<>(cookies.size());
    for (final Cookie cookie: cookies) {
      cookieMap.put(cookie.name(), cookie.value());
    }
    return cookieMap;
  }

  public static String sanitizePath(final HttpMessageRequest request, final String uriPrefix) throws MessageException {
    final String path = UriUtil.sanitizeUri(uriPrefix, request.path());
    if (path == null) {
      Logger.warn("forbidden file request {} uriPrefix {} - sanitization", request.path(), uriPrefix);
      throw new MessageException(MessageError.newForbidden("path sanitization failed"));
    }
    return path;
  }

  private static final class HttpMessageQueryParams implements MessageMetadata {
    private final Map<String, List<String>> queryParams;

    private HttpMessageQueryParams(final Map<String, List<String>> queryParams) {
      this.queryParams = queryParams;
    }

    @Override
    public int size() {
      return queryParams.size();
    }

    @Override
    public boolean isEmpty() {
      return queryParams.isEmpty();
    }

    @Override
    public String get(final String key) {
      final List<String> params = queryParams.get(key);
      return ListUtil.isEmpty(params) ? null : params.get(0);
    }

    @Override
    public List<String> getList(final String key) {
      return queryParams.get(key);
    }

    @Override
    public void forEach(final BiConsumer<? super String, ? super String> action) {
      for (final Entry<String, List<String>> entry: queryParams.entrySet()) {
        for (final String value: entry.getValue()) {
          action.accept(entry.getKey(), value);
        }
      }
    }
  }
}
