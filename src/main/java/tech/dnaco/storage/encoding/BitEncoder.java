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
import java.io.OutputStream;

import tech.dnaco.bytes.encoding.IntEncoder;

public class BitEncoder implements AutoCloseable {
  private final OutputStream stream;
  private final long mask;
  private final int width;

  private long vBuffer = 0;
  private int availBits;
  private int vBits = 0;

  public BitEncoder(final OutputStream stream, final int width) {
    this.stream = stream;
    this.width = width;
    this.mask = (width == 64) ? 0xffffffffffffffffL : ((1L << width) - 1);
    this.availBits = Long.BYTES << 3;
  }

  @Override
  public void close() throws IOException {
    if (availBits != (Long.BYTES << 3)) {
      flush();
    }
  }

  public void add(final int v) throws IOException {
    if (availBits == 0) flush();

    //System.out.println("ADD " + Long.toBinaryString(v) + " MASK " + Long.toBinaryString(mask) + " -> " + Long.toBinaryString(vBuffer));
    if (availBits >= width) {
      vBuffer = (vBuffer << width) | (v & mask);
      availBits -= width;
      vBits += width;
      return;
    }

    final int v1Width = (width - availBits);
    final long maskV0 = (1L << availBits) - 1;
    final long maskV1 = (1L << v1Width) - 1;

    final long v0 = (v >> v1Width) & maskV0;
    final long v1 = v & maskV1;

    //System.out.println("AVAIL-BITS " + availBits);
    //System.out.println(" - v0: " + Long.toBinaryString(v0));
    //System.out.println(" - v1: " + Long.toBinaryString(v1));
    vBuffer = (vBuffer << availBits) | v0;
    vBits += availBits;
    availBits = 0;
    flush();

    vBuffer = v1;
    availBits = (Long.BYTES << 3) - v1Width;
    vBits += v1Width;
  }

  public void flush() throws IOException {
    //System.out.println("ENCODE vBuffer=" + vBuffer + " -> " + Long.toBinaryString(vBuffer) + " bits=" + vBits + " bytes=" + ((vBits + 7) / 8));
    IntEncoder.BIG_ENDIAN.writeFixed(stream, vBuffer, (vBits + 7) >> 3);
    availBits = Long.BYTES << 3;
    vBuffer = 0;
    vBits = 0;
  }
}
