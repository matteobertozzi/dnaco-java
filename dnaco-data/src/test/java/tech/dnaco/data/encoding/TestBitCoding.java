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

package tech.dnaco.data.encoding;

import java.util.Random;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.dnaco.collections.arrays.LongArray;

public class TestBitCoding {
  @Test
  public void testRandWriteRead() {
    final int N = 500_000;

    final Random rand = new Random();
    final LongArray values = new LongArray(N);

    final BitEncoder encoder = new BitEncoder(1 << 20);
    for (int i = 0; i < (N / 2); ++i) {
      final int bits = 1 + rand.nextInt(64);
      final long value;
      if (bits == 64) {
        value = Long.MAX_VALUE + rand.nextInt(0xff);
      } else {
        value = rand.nextLong((1L << bits) - 1);
      }
      encoder.add(value, bits);

      values.add(bits);
      values.add(value);
    }
    encoder.flush();

    final BitDecoder decoder = new BitDecoder(encoder.buffer().toByteArray());
    for (int i = 0; i < values.size(); i += 2) {
      final int bits = Math.toIntExact(values.get(i));
      final long expectedValue = values.get(i + 1);
      final long value = decoder.read(bits);
      if (bits == 64) {
        Assertions.assertEquals(0, Long.compareUnsigned(expectedValue, value));
      } else {
        Assertions.assertEquals(expectedValue, value);
      }
    }
  }
}
