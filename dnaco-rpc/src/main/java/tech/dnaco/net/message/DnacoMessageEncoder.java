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

package tech.dnaco.net.message;

import java.util.List;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

@Sharable
public class DnacoMessageEncoder extends MessageToMessageEncoder<DnacoMessage> {
  public static final DnacoMessageEncoder INSTANCE = new DnacoMessageEncoder();

  private DnacoMessageEncoder() {
    // no-op
  }

  @Override
  protected void encode(final ChannelHandlerContext ctx, final DnacoMessage msg, final List<Object> out) throws Exception {
    out.add(DnacoMessageUtil.encodeMessage(msg));
  }
}
