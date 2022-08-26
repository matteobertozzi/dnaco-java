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

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

public class HttpResponseUtil {
  private static final boolean KEEP_ALIVE_SUPPORTED = true;

  private HttpResponseUtil() {
    // no-op
  }

  public static void writeEmptyResponse(final ChannelHandlerContext ctx, final HttpResponseStatus status) {
    writeEmptyResponse(ctx, status, false);
  }

  /*
  public static void writeBasicAuthRequired(final ChannelHandlerContext ctx, final String realm) {
    writeBasicAuthRequired(ctx, realm, "authentication required");
  }

  public static void writeBasicAuthRequired(final ChannelHandlerContext ctx, final String realm,
      final String errorPageContent) {
    final HttpHeaders headers = new DefaultHttpHeaders();
    headers.set(HttpHeaderNames.WWW_AUTHENTICATE, "Basic realm=\"" + realm + "\"");
    ctx.writeResponse(HttpResponseStatus.UNAUTHORIZED, headers, errorPageContent);
  }

  public static void writeBasicAuthRequired(final ChannelHandlerContext ctx, final String realm,
      final byte[] errorPageContent) {
    final HttpHeaders headers = new DefaultHttpHeaders();
    headers.set(HttpHeaderNames.WWW_AUTHENTICATE, "Basic realm=\"" + realm + "\"");
    ctx.writeResponse(HttpResponseStatus.UNAUTHORIZED, headers, errorPageContent);
  }

  private static void file(final HttpHeaders headers, final long lastModified) {
    // Cache Validation
    final String ifModifiedSince = headers.get(HttpHeaderNames.IF_MODIFIED_SINCE);
    if (ifModifiedSince != null && !ifModifiedSince.isEmpty()) {
      final ZonedDateTime zdt = ZonedDateTime.parse(ifModifiedSince, DateTimeFormatter.RFC_1123_DATE_TIME);

      // Only compare up to the second because the datetime format
      // we send to the client does not have milliseconds
      final long ifModifiedSinceDateSeconds = zdt.toInstant().toEpochMilli() / 1000;
      final long fileLastModifiedSeconds = lastModified / 1000;
      if (ifModifiedSinceDateSeconds == fileLastModifiedSeconds) {
        writeEmptyResponse(HttpResponseStatus.NOT_MODIFIED, keepAlive);
        return;
      }
    }
  }
  */

  public static void writeEmptyResponse(final ChannelHandlerContext ctx,
      final HttpResponseStatus status, final boolean keepAlive) {
    final FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, Unpooled.EMPTY_BUFFER);
    response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 0);
    writeAndFlush(ctx, response, keepAlive);
  }

  private static void writeAndFlush(final ChannelHandlerContext ctx,
      final FullHttpResponse response, final boolean keepAlive) {
    // handle keep alive flag
    if (KEEP_ALIVE_SUPPORTED && keepAlive) {
      response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
      ctx.writeAndFlush(response, ctx.channel().voidPromise());
    } else {
      ctx.writeAndFlush(response);
    }
  }

  //FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, NOT_MODIFIED, Unpooled.EMPTY_BUFFER);
}
