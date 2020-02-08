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

package tech.dnaco.server.pu;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import tech.dnaco.logging.Logger;
import tech.dnaco.server.binary.BinaryPacket;

public final class PortUnificationServerHandler extends ByteToMessageDecoder {
  private final boolean detectGzip;

  public PortUnificationServerHandler() {
    this(true);
  }

  private PortUnificationServerHandler(final boolean detectGzip) {
    this.detectGzip = detectGzip;
  }

  @Override
  protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out)
      throws Exception {
    // Will use the first 2 bytes to detect a protocol.
    if (in.readableBytes() < 2) return;

    final int magic1 = in.getUnsignedByte(in.readerIndex());
    final int magic2 = in.getUnsignedByte(in.readerIndex() + 1);

    if (isGzip(magic1, magic2)) {
      enableGzip(ctx);
    //} else if (NettyHttpHandler.isHttpRequest(magic1, magic2)) {
      //switchToHttp(ctx);
    } else if (BinaryPacket.isBinaryPacket(magic1, magic2)) {
      switchToBinaryCommand(ctx);
    } else {
      // Unknown protocol; discard everything and close the connection.
      Logger.warn("unknown protocol starting with {} {}",
        Integer.toHexString(magic1), Integer.toHexString(magic2));
      in.clear();
      ctx.close();
    }
  }

  private boolean isGzip(final int magic1, final int magic2) {
    if (detectGzip) {
      return magic1 == 31 && magic2 == 139;
    }
    return false;
  }

  private void enableGzip(final ChannelHandlerContext ctx) {
    final ChannelPipeline p = ctx.pipeline();
    p.addLast("gzipDeflater", ZlibCodecFactory.newZlibEncoder(ZlibWrapper.GZIP));
    p.addLast("gzipInflater", ZlibCodecFactory.newZlibDecoder(ZlibWrapper.GZIP));
    p.addLast("unification", new PortUnificationServerHandler(false));
    p.remove(this);
  }

  private void switchToHttp(final ChannelHandlerContext ctx) {
    final ChannelPipeline p = ctx.pipeline();
    //NettyHttpHandler.initHttpPipeline(p);
    p.remove(this);
  }

  private void switchToBinaryCommand(final ChannelHandlerContext ctx) {
    final ChannelPipeline p = ctx.pipeline();
    //BinaryCommandHandler.initPipeline(p);
    p.remove(this);
  }
}
