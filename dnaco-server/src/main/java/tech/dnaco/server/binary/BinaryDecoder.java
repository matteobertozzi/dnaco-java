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

package tech.dnaco.server.binary;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class BinaryDecoder extends ByteToMessageDecoder {
  private static final int MIN_BYTES = 4 + 8 + 4 + 4;

  @Override
  protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out) throws Exception {
    while (true) {
      if (in.readableBytes() < MIN_BYTES) break;
      if (in.readableBytes() < (MIN_BYTES + in.getInt(16))) break;

      final int magic = in.readInt();
      if (magic != BinaryPacket.MAGIC) {
        in.clear();
        ctx.close();
        return;
      }

      final long pkgId = in.readLong();
      final int command = in.readInt();
      final int dataLength = in.readInt();
      final ByteBuf data = in.readRetainedSlice(dataLength);
      out.add(BinaryPacket.alloc(pkgId, command, data));
    }
  }
}
