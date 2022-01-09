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

import tech.dnaco.collections.LongValue;
import tech.dnaco.collections.arrays.LongArray;

public class TestIntSeqCoding {
  @Test
  public void testSeqRandWriteRead() {
    final int N = 100;

    final Random rand = new Random();
    final LongArray values = new LongArray(N);
    final BitEncoder bitEncoder = new BitEncoder(1 << 10);
    while (values.size() < N) {
      final int length = 1 + rand.nextInt(1 + ((N - values.size()) / 2));
      switch (IntSeqCoding.SLICE_TYPES[rand.nextInt(IntSeqCoding.SLICE_TYPES.length - 1)]) {
        case RLE: {
          final long value = rand.nextLong(Long.MAX_VALUE);
          for (int i = 0; i < length; ++i) values.add(value);
          IntSeqCoding.writeRle(bitEncoder, length, value);
          break;
        }
        case LIN: {
          final long baseValue = rand.nextLong(Long.MAX_VALUE);
          final long delta = rand.nextLong(Integer.MAX_VALUE);
          for (int i = 0; i < length; ++i) values.add(baseValue + (i * delta));
          IntSeqCoding.writeLin(bitEncoder, length, baseValue, delta);
          break;
        }
        case MIN: {
          long minValue = Long.MAX_VALUE;
          long maxValue = 0;
          for (int i = 0; i < length; ++i) {
            final long value = rand.nextLong(Integer.MAX_VALUE);
            minValue = Math.min(minValue, value);
            maxValue = Math.max(maxValue, value);
            values.add(value);
          }
          IntSeqCoding.writeMin(bitEncoder, values.buffer(), values.size() - length, values.size(), minValue, maxValue - minValue);
          break;
        }
        case EOF:
          break;
      }
    }
    bitEncoder.add(IntSeqCoding.SliceType.EOF.ordinal(), 2);
    bitEncoder.flush();

    final LongValue index = new LongValue();
    IntSeqCoding.decode(bitEncoder.buffer().toByteArray(), 0, value -> {
      Assertions.assertEquals(value, values.get(index.intValue()));
      index.incrementAndGet();
    });
    Assertions.assertEquals(values.size(), index.intValue());
  }
}
