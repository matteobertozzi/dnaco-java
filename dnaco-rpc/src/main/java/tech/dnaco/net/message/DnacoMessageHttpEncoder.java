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

package tech.dnaco.net.message;

import java.util.List;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import tech.dnaco.dispatcher.message.MessageMetadataMap;
import tech.dnaco.logging.Logger;

@Sharable
public class DnacoMessageHttpEncoder extends MessageToMessageEncoder<DnacoMessage> {
  public static final DnacoMessageHttpEncoder INSTANCE = new DnacoMessageHttpEncoder();

  private DnacoMessageHttpEncoder() {
    // no-op
  }

  @Override
  protected void encode(final ChannelHandlerContext ctx, final DnacoMessage msg, final List<Object> out) throws Exception {
    Logger.debug("send message response as http: {}", msg);
    out.add(encode(msg));
  }

  public static FullHttpResponse encode(final DnacoMessage msg) {
    final MessageMetadataMap metadata = msg.metadataMap();
    final HttpResponseStatus status = HttpResponseStatus.valueOf(metadata.getInt(DnacoMessageUtil.METADATA_FOR_HTTP_STATUS, -1));
    //final HttpResponseStatus status = HttpResponseStatus.OK;
    final DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, msg.data().retain());
    //response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 19);
          //response.headers().set(HttpHeaderNames.SERVER, "Armeria/1.16.0");
          //response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=utf-8");
          //response.headers().set(HttpHeaderNames.DATE, new Date());
    encodeHeaders(response.headers(), metadata);
    return response;
  }

  public static FullHttpRequest encodeAsRequest(final DnacoMessage msg) {
    final MessageMetadataMap metadata = msg.metadataMap();

    final HttpMethod method = HttpMethod.valueOf(metadata.get(DnacoMessageUtil.METADATA_FOR_HTTP_METHOD));
    final String uri = metadata.get(DnacoMessageUtil.METADATA_FOR_HTTP_URI);
    final DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, uri, msg.data().retain());
    encodeHeaders(request.headers(), metadata);
    return request;
  }

  private static void encodeHeaders(final HttpHeaders headers, final MessageMetadataMap metadata) {
    metadata.forEach((k, v) -> {
      //System.out.println("META KEY " + entry.getKey());
      if (!DnacoMessageUtil.isMetaKeyReserved(k)) {
        headers.add(k, v);
      }
    });
  }
}
