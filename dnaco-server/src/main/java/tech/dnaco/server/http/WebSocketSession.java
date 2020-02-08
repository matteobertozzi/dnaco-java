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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import tech.dnaco.server.AbstractServiceSession;

public class WebSocketSession extends AbstractServiceSession {
  protected WebSocketSession(final ChannelHandlerContext ctx) {
    super(ctx);
  }

  public void sendText(final String message) {
    writeAndFlush(new TextWebSocketFrame(message));
  }

  public void sendBinary(final byte[] buf) {
    sendBinary(Unpooled.wrappedBuffer(buf));
  }

  public void sendBinary(final byte[] buf, final int off, final int len) {
    sendBinary(Unpooled.wrappedBuffer(buf, off, len));
  }

  public void sendBinary(final ByteBuf message) {
    writeAndFlush(new BinaryWebSocketFrame(message));
  }

  public void kill() {
    writeAndFlush(new CloseWebSocketFrame());
  }
}
