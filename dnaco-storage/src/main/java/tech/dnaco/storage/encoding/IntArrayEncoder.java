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
import tech.dnaco.bytes.encoding.IntUtil;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.storage.encoding.BitEncoder;

public final class IntArrayEncoder {
  private IntArrayEncoder() {
    // no-op
  }

  public static void encodeSequence(final OutputStream writer, final int[] buf, final int off, final int len)
      throws IOException {
    int minValue = buf[off];
    int maxValue = minValue;
    for (int i = 1; i < len; ++i) {
      final int v = buf[off + i] - buf[off + i - 1];
      minValue = Math.min(minValue, v);
      maxValue = Math.max(maxValue, v);
    }
    final int maxWidth = IntUtil.getWidth(maxValue);

    final int width = IntUtil.getWidth(maxValue - minValue);

    if (false) {
      System.out.println(" --> INDEX MIN-VALUE " + minValue + " MAX-VALUE " + maxValue +
                         " WIDTH " + maxWidth + " -> " + width +
                         " -> " + width + " total " + HumansUtil.humanSize(((len * width) + 7) / 8));
    }
    IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(writer, len);
    IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(writer, minValue);
    IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(writer, width);
    try (BitEncoder encoder = new BitEncoder(writer, width)) {
      encoder.add(buf[off] - minValue);
      for (int i = 1; i < len; ++i) {
        encoder.add((buf[off + i] - buf[off + i - 1]) - minValue);
      }
    }
  }
}
