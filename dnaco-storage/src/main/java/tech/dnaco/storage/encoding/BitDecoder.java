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

import tech.dnaco.bytes.ByteArrayReader;
import tech.dnaco.bytes.ByteArrayWriter;
import tech.dnaco.bytes.encoding.IntDecoder;

public class BitDecoder {
  private static final int[] BYTES_PER_WIDTH = new int[] {
      0,
      1,  1,  3,  1,  5,  3,  7,  1, // 8
      9,  5, 11,  3, 13,  7, 15,  2, // 16
     17,  9, 19,  5, 21, 11, 23,  3, // 24
     25, 13, 27,  7, 29, 15, 31,  4, // 32
     33, 17, 35,  9, 37, 19, 39,  5, // 40
     41, 21, 43, 11, 45, 23, 47,  6, // 48
     49, 25, 51, 13, 53, 27, 55,  7, // 56
     57, 29, 59, 15, 61, 31, 63,  8, // 64
  };

  private final InputStream stream;
  private final int width;

  private long vBuffer = 0;
  private int vCount = 0;

  public BitDecoder(final InputStream stream, final int width) {
    this.stream = stream;
    this.width = width;
  }

  public int readInt() throws IOException {
    return (int) read();
  }

  public long read() throws IOException {
    if (vCount == 0) {
      final int bpw = BYTES_PER_WIDTH[width];
      vBuffer = IntDecoder.BIG_ENDIAN.readFixed(stream, bpw);
      vCount = (bpw * 8) / width;
    }
    final int mask = (1 << width) - 1;
    vCount--;
    return vBuffer >> (vCount * width) & mask;
  }

  public static void main(final String[] args) throws IOException {
    try (ByteArrayWriter writer = new ByteArrayWriter(new byte[512])) {
      try (BitEncoder encoder = new BitEncoder(writer, 3)) {
        for (int i = 0; i < 12; ++i) {
          encoder.add(i % 3);
        }
      }

      System.out.println("WRITE SIZE " + writer.writeOffset());
      try (ByteArrayReader reader = new ByteArrayReader(writer.rawBuffer(), writer.offset(), writer.writeOffset())) {
        final BitDecoder decoder = new BitDecoder(reader, 3);
        for (int i = 0; i < 12; ++i) {
          System.out.println(decoder.read());
        }
      }
    }
  }
}
