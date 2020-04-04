/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.dnaco.server.http;

import java.util.Date;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import tech.dnaco.logging.Logger;
import tech.dnaco.util.JsonUtil;

public final class HttpResponseUtil {
  private HttpResponseUtil() {
    // no-op
  }

	public static void writeTextResponse(final ChannelHandlerContext ctx, final HttpResponseStatus status, final String msg) {
		final byte[] data = msg.getBytes();
    final FullHttpResponse response = prepareResponse(ctx, status, data);
		response.headers().add(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN);
		response.headers().add(HttpHeaderNames.CONTENT_LENGTH, data.length);
    ctx.writeAndFlush(response);
  }

	public static void writeJsonResponse(final ChannelHandlerContext ctx, final HttpResponseStatus status, final Object msg) {
    final byte[] json = JsonUtil.toJson(msg).getBytes();
    final FullHttpResponse response = prepareResponse(ctx, status, json);
		response.headers().add(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
		response.headers().add(HttpHeaderNames.CONTENT_LENGTH, json.length);
    ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
  }

  private static FullHttpResponse prepareResponse(final ChannelHandlerContext ctx, final HttpResponseStatus status, final byte[] body) {
    return prepareResponse(ctx, status, Unpooled.wrappedBuffer(body));
  }

  private static FullHttpResponse prepareResponse(final ChannelHandlerContext ctx, final HttpResponseStatus status, final ByteBuf body) {
    final FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, body);
    response.headers().add("X-TraceId", Logger.getSessionTraceId());
		response.headers().add(HttpHeaderNames.DATE, new Date());
    return response;
  }
}
