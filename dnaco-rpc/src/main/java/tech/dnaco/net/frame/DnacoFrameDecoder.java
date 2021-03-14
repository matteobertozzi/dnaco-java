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

package tech.dnaco.net.frame;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SingleThreadEventLoop;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.concurrent.EventExecutor;

public class DnacoFrameDecoder extends ByteToMessageDecoder {
  @Override
  protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out) {
    final boolean hasMore = DnacoFrameUtil.decodeFrames(in, 128, (frame) -> {
      out.add(frame);
      DnacoFrameStats.INSTANCE.addReadFrame(DnacoFrameUtil.HEADER_SIZE + frame.getLength());
    });
    // stop reading if growing too much?
    //ctx.channel().config().setAutoRead(true);
  }

  private int pendingTasks(final EventExecutor executor) {
    if (executor instanceof SingleThreadEventLoop) {
      return ((SingleThreadEventLoop)executor).pendingTasks();
    }
    return 0;
  }
}
