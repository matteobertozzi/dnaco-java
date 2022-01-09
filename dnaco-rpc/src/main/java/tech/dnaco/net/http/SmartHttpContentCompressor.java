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

import java.util.List;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import tech.dnaco.data.util.MimeUtil;
import tech.dnaco.strings.StringConverter;
import tech.dnaco.strings.StringUtil;

/**
 * Better version of {@link HttpContentCompressor} which can be disabled by setting
 * Content-Encoding: Identity for a response.
 * <p>
 * Also it disables itself if the given content is not compressable (jpg, png)
 * or too small (less than 1 kB).
 * </p>
 */
public class SmartHttpContentCompressor extends HttpContentCompressor {
  private boolean passThrough = false;

  @Override
  protected void encode(final ChannelHandlerContext ctx, final HttpObject msg, final List<Object> out) throws Exception {
    if (msg instanceof final HttpResponse response) {
      passThrough = shouldSkipCompression(response);
    }
    super.encode(ctx, msg, out);
  }

  @Override
  protected Result beginEncode(final HttpResponse headers, final String acceptEncoding) throws Exception {
    // If compression is skipped, we return null here which disables the compression effectively...
    if (passThrough) {
      return null;
    }
    return super.beginEncode(headers, acceptEncoding);
  }

  private static boolean shouldSkipCompression(final HttpResponse response) {
    // If "Content-Encoding" header was set, we do not compress
    final String contentEncoding = response.headers().get(HttpHeaderNames.CONTENT_ENCODING);
    if (StringUtil.isNotEmpty(contentEncoding)) {
      if (HttpHeaderValues.IDENTITY.contentEquals(contentEncoding)) {
        // Remove header as one SHOULD NOT send Identity as content encoding.
        response.headers().remove(HttpHeaderNames.CONTENT_ENCODING);
      }
      return true;
    }

    // If the content type is not compressable (jpg, png ...), we skip compression
    final String contentType = response.headers().get(HttpHeaderNames.CONTENT_TYPE);
    if (MimeUtil.INSTANCE.isCompressable(contentType)) {
      // If the content length is less than 1 kB but known, we also skip compression
      final int contentLength = StringConverter.toInt(response.headers().get(HttpHeaderNames.CONTENT_LENGTH), 0);
      return contentLength > 0 && contentLength < 1024;
    }

    return true;
  }
}
