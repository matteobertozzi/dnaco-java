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
import java.util.List;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.FullHttpResponse;
import tech.dnaco.data.DataFormat;
import tech.dnaco.dispatcher.message.Message;
import tech.dnaco.dispatcher.message.MessageMetadata;
import tech.dnaco.net.util.ByteBufDataFormatUtil;

public class HttpMessageResponse implements Message {
  private final HttpMessageMetadata metadata;
  private final FullHttpResponse response;

  protected HttpMessageResponse(final FullHttpResponse response) {
    this.response = response;
    this.metadata = new HttpMessageMetadata(response.headers());
  }

  @Override
  public Message retain() {
    response.retain();
    return this;
  }


  @Override
  public Message release() {
    response.release();
    return this;
  }

  protected FullHttpResponse rawResponse() {
    return response;
  }

  @Override
  public int contentLength() {
    return response.content().readableBytes();
  }

  @Override
  public long writeContentToStream(final OutputStream stream) throws IOException {
    return ByteBufDataFormatUtil.transferTo(response.content(), stream);
  }

  @Override
  public long writeContentToStream(final DataOutput stream) throws IOException {
    return ByteBufDataFormatUtil.transferTo(response.content(), stream);
  }

  @Override
  public <T> T convertContent(final DataFormat format, final Class<T> classOfT) {
    return ByteBufDataFormatUtil.fromBytes(format, response.content(), classOfT);
  }

  @Override
  public int estimateSize() {
    return 4 + metadata.estimateSpace() + 4 + response.content().readableBytes();
  }

  @Override
  public MessageMetadata metadata() {
    return metadata;
  }

  protected void write(final ChannelHandlerContext ctx) {
    ctx.write(response);
  }

  @Sharable
  public static final class HttpMessageResponseEncoder extends MessageToMessageEncoder<HttpMessageResponse> {
    public static final HttpMessageResponseEncoder INSTANCE = new HttpMessageResponseEncoder();

    private HttpMessageResponseEncoder() {
      // no-op
    }

    @Override
    protected void encode(final ChannelHandlerContext ctx, final HttpMessageResponse msg, final List<Object> out) throws Exception {
      out.add(msg.response);
    }
  }
}
