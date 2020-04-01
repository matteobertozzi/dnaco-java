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

package tech.dnaco.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

public class AbstractServiceSession {
  private final ChannelHandlerContext ctx;

  protected AbstractServiceSession(final ChannelHandlerContext ctx) {
    this.ctx = ctx;
  }

  protected void write(final Object msg) {
    ctx.write(msg);
  }

  protected void writeAndFlush(final Object msg) {
    ctx.writeAndFlush(msg);
  }

  protected void disconnect() {
    ctx.close();
  }

  protected <T> Attribute<T> attr(final AttributeKey<T> key) {
    return ctx.channel().attr(key);
  }
}