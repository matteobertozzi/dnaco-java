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

public final class IntArrayDecoder {
  private IntArrayDecoder() {
    // no-op
  }

  public static int[] decodeSequence(final InputStream stream, final int prefix)
      throws IOException {
    final int len = IntDecoder.BIG_ENDIAN.readUnsignedVarInt(stream);
    if (len == 0) return new int[prefix];

    final int minValue = IntDecoder.BIG_ENDIAN.readUnsignedVarInt(stream);
    if (len == 1) {
      final int[] data = new int[prefix + 1];
      data[prefix] = minValue;
      return data;
    }

    final int width = IntDecoder.BIG_ENDIAN.readUnsignedVarInt(stream);
    //System.out.println("DECODE");
    //System.out.println(" - len=" + len);
    //System.out.println(" - minValue=" + minValue);
    //System.out.println(" - width=" + width);
    final int[] data = new int[prefix + len];
    final BitDecoder bitDecoder = new BitDecoder(stream, width, len);
    data[prefix] = bitDecoder.readInt() + minValue;
    for (int i = 1; i < len; ++i) {
      data[prefix + i] = minValue + data[(prefix + i) - 1] + bitDecoder.readInt();
    }
    return data;
  }
}
