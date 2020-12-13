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

package tech.dnaco.storage.encoding;

import java.io.IOException;
import java.io.InputStream;

import tech.dnaco.bytes.encoding.IntDecoder;

public class BitDecoder {
  private final InputStream stream;
  private final long mask;
  private final int width;

  private long vBuffer;
  private int availBits;
  private int remainingBits;

  public BitDecoder(final InputStream stream, final int width, final int length) {
    this.stream = stream;
    this.width = width;
    this.mask = (width == 64) ? 0xffffffffffffffffL : ((1L << width) - 1);
    this.availBits = 0;
    this.remainingBits = length * width;
  }

  public int readInt() throws IOException {
    return (int) read();
  }

  public long read() throws IOException {
    if (availBits >= width) {
      final long v = (vBuffer >> (availBits - width)) & mask;
      //System.out.println(" ---> READ0 availBits=" + availBits + " v=" + v + " -> " + Long.toBinaryString(v) + " -> " + (availBits - width));
      remainingBits -= width;
      availBits -= width;
      return v;
    }

    final long overflowMask = (1L << availBits) - 1;
    final int widthRemaining = width - availBits;
    long v = (vBuffer & overflowMask) << widthRemaining;
    remainingBits -= availBits;
    //System.out.println(" -------> OVERFLOW " + v + " widthRemaining=" + widthRemaining);

    final int vBits = Math.min(Long.BYTES << 3, remainingBits);
    //System.out.println("READ-FIXED vBits=" + vBits + " READ-BYTES=" + ((vBits + 7) >> 3));
    vBuffer = IntDecoder.BIG_ENDIAN.readFixed(stream, (vBits + 7) >> 3);
    availBits = vBits;
    //System.out.println("FETCH " + vBuffer + " vBits=" + vBits + " -> " + Long.toBinaryString(vBuffer) + " availBits=" + availBits + " remainingBits=" + remainingBits);

    v |= (vBuffer >> (availBits - widthRemaining)) & ((1L << widthRemaining) - 1);
    //System.out.println(" ---> READ1 availBits=" + availBits + " v=" + v + " -> " + Long.toBinaryString(v));
    remainingBits -= widthRemaining;
    availBits -= widthRemaining;
    return v;
  }
}
