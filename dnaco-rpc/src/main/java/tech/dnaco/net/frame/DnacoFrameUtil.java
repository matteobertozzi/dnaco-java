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

import java.util.function.Consumer;

import io.netty.buffer.ByteBuf;

public final class DnacoFrameUtil {
  public static final int MAX_FRAME_SIZE = 128 << 20;
  public static final int HEADER_SIZE = 4;

  private DnacoFrameUtil() {
    // no-op
  }

  // FRAME
  // frames are in the form of | length (u32) | ...data... |
  // - the first 5bit are used to identify the protocol version (0-31).
  // - the following 27bits are used to identify the packet length (max 128M)
  //   +-------+--------------------------------------+
  //   | 11111 | 111 | 11111111 | 11111111 | 11111111 |
  //   +-------+--------------------------------------+
  //   0 rev.  5             data length             32
  public static DnacoFrame decodeFrame(final ByteBuf in) {
    final int avail = in.readableBytes();
    if (avail < HEADER_SIZE) return null;

    // look at the header and check if the frame length is available
    final int frameHeader = in.getInt(in.readerIndex());
    final int frameLength = DnacoFrame.readDataLength(frameHeader);
    //System.out.println("FRAME LEN " + frameLength + " AVAIL " + avail);
    if (avail < frameLength + HEADER_SIZE) return null;

    // read the frame data
    in.skipBytes(DnacoFrameUtil.HEADER_SIZE);
    final int rev = DnacoFrame.readRev(frameHeader);
    final ByteBuf frameData = in.readRetainedSlice(frameLength);
    return DnacoFrame.alloc(rev, frameData);
  }

  public static boolean decodeFrames(final ByteBuf in, final int limit, final Consumer<DnacoFrame> consumer) {
    for (int i = 0; i < limit; ++i) {
      final DnacoFrame frame = decodeFrame(in);
      if (frame == null) return false;

      //System.out.println("FRAME " + frame + " -> " + ByteBufUtil.hexDump(frame.getData()));
      consumer.accept(frame);
    }
    return true;
  }

  public static void encodeFrame(final DnacoFrame frame, final ByteBuf out) {
    out.writeInt(frame.getHeader());
    out.writeBytes(frame.getData());
  }
}
