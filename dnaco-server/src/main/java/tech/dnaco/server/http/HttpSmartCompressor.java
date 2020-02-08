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

import java.util.List;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import tech.dnaco.server.util.MimeUtil;
import tech.dnaco.strings.StringConverter;

/**
 * Better version of {@link HttpContentCompressor} which can be disabled by setting
 * Content-Encoding: Identity for a response.
 * <p>
 * Also it disables itself if the given content is not compressable (jpg, png) or too small (less
 * than 1 kB).
 * </p>
 */
public class HttpSmartCompressor extends HttpContentCompressor {
  private boolean passThrough;

  @Override
  protected void encode(final ChannelHandlerContext ctx, final HttpObject msg, final List<Object> out) throws Exception {
    if (msg instanceof HttpResponse) {
      // Check if this response should be compressed
      final HttpResponse res = (HttpResponse) msg;
      // by default compression is on (passThrough bypasses compression)
      passThrough = false;
      // If an "Content-Encoding: Identity" header was set, we do not compress
      if (res.headers().contains(HttpHeaderNames.CONTENT_ENCODING, HttpHeaderValues.IDENTITY, true)) {
        passThrough = true;
        // Remove header as one SHOULD NOT send Identity as content encoding.
        res.headers().remove(HttpHeaderNames.CONTENT_ENCODING);
      } else {
        // If the content type is not compressable (jpg, png ...), we skip compression
        final String contentType = res.headers().get(HttpHeaderNames.CONTENT_TYPE);
        if (MimeUtil.INSTANCE.isCompressable(contentType)) {
          // If the content length is less than 1 kB but known, we also skip compression
          final int contentLength = StringConverter.toInt(res.headers().get(HttpHeaderNames.CONTENT_LENGTH), 0);
          passThrough = (contentLength > 0 && contentLength < 1024);
        } else {
          passThrough = true;
        }
      }
    }
    super.encode(ctx, msg, out);
  }

  @Override
  protected Result beginEncode(final HttpResponse headers, final String acceptEncoding) throws Exception {
    // If compression is skipped, we return null here which disables the compression effectively...
    return passThrough ? null : super.beginEncode(headers, acceptEncoding);
  }
}
