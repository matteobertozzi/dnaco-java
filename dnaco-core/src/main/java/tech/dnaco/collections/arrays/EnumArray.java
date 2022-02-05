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

package tech.dnaco.collections.arrays;

import java.util.function.Consumer;

import tech.dnaco.bytes.encoding.IntUtil;
import tech.dnaco.util.BitUtil;

public class EnumArray<T extends Enum<T>> {
  private final LongArray values = new LongArray(16);
  private final T[] universe;
  private final int bits;

  private long vBuffer = 0;
  private int vBitsAvail = Long.SIZE;

  public EnumArray(final Class<T> elementType) {
    this(elementType.getEnumConstants());
  }

  public EnumArray(final T[] universe) {
    this.universe = universe;
    for (int i = 0; i < universe.length; ++i) {
      if (universe[i].ordinal() != i) {
        throw new IllegalArgumentException("expected ordinals from 0-N: " + universe[i]);
      }
    }
    this.bits = IntUtil.getWidth(universe.length);
  }

  public void add(final T value) {
    vBitsAvail -= bits;
    vBuffer |= ((long)value.ordinal() << vBitsAvail);
    if (vBitsAvail < bits) {
      values.add(vBuffer);
      vBuffer = 0;
      vBitsAvail = Long.SIZE;
    }
  }

  public T get(final int index) {
    final int itemsPerBlock = Long.SIZE / bits;
    final int block = index / itemsPerBlock;
    final int offset = Long.SIZE - bits - ((index % itemsPerBlock) * bits);

    final long rBuffer;
    if (block == values.size()) {
      rBuffer = vBuffer;
    } else {
      rBuffer = values.get(block);
    }

    final int ordinal = (int) ((rBuffer >>> offset) & BitUtil.mask(bits));
    return universe[ordinal];
  }

  public int size() {
    return ((values.size() * Long.SIZE) / bits) + ((Long.SIZE - vBitsAvail) / bits);
  }


  public void foreach(final Consumer<T> consumer) {
    final long mask = BitUtil.mask(bits);
    values.forEach(v -> {
      for (int rBits = Long.SIZE; rBits > 0; rBits -= bits) {
        final int ordinal = (int)((v >>> (rBits - bits)) & mask);
        consumer.accept(universe[ordinal]);
      }
    });

    for (int rBits = Long.SIZE; rBits > vBitsAvail; rBits -= bits) {
      final int ordinal = (int)((vBuffer >>> (rBits - bits)) & mask);
      consumer.accept(universe[ordinal]);
    }
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("[");
    foreach(value -> builder.append(value).append(", "));
    builder.setLength(builder.length() - 2);
    builder.append("]");
    return builder.toString();
  }
}
