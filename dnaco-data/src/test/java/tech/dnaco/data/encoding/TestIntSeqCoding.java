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
  public void testMinSeq() {
    final long[] seq = new long[] {
      962264894997320L, 25764,
      6310565, 27729, 15466364862304L, 444585871691175L, 484869823320558L, 67, 30287832, 21449
    };

    // type:MIN start:0 end:2 value:25764 delta:962264894971556
    // type:MIN start:2 end:7 value:27729 delta:484869823292829
    // type:MIN start:7 end:10 value:67 delta:30287765

    final BitEncoder bitEncoder = new BitEncoder(1 << 10);
    IntSeqCoding.writeMin(bitEncoder, seq, 0,  2, 25764, 962264894971556L);
    IntSeqCoding.writeMin(bitEncoder, seq, 2,  7, 27729, 484869823292829L);
    IntSeqCoding.writeMin(bitEncoder, seq, 7, 10, 67, 30287765);
    bitEncoder.add(IntSeqCoding.IntSeqSliceType.EOF.ordinal(), 2);
    bitEncoder.flush();

    final LongValue index = new LongValue();
    IntSeqCoding.decode(bitEncoder.buffer().toByteArray(), 0, value -> {
      Assertions.assertEquals(value, seq[index.intValue()]);
      index.incrementAndGet();
    });
    Assertions.assertEquals(seq.length, index.intValue());
  }


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
    bitEncoder.add(IntSeqCoding.IntSeqSliceType.EOF.ordinal(), 2);
    bitEncoder.flush();

    final LongValue index = new LongValue();
    IntSeqCoding.decode(bitEncoder.buffer().toByteArray(), 0, value -> {
      Assertions.assertEquals(value, values.get(index.intValue()));
      index.incrementAndGet();
    });
    Assertions.assertEquals(values.size(), index.intValue());
  }
}
