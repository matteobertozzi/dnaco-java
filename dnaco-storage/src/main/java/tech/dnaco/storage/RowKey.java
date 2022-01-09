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

package tech.dnaco.storage;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.bytes.encoding.IntEncoder;
import tech.dnaco.bytes.encoding.VarInt;
import tech.dnaco.collections.LongValue;
import tech.dnaco.collections.arrays.ByteArray;
import tech.dnaco.collections.arrays.IntArray;

public final class RowKey {
  private static final byte[] ZERO = new byte[] { 0, 0 };

  private final IntArray index;
  private final byte[] key;

  public RowKey(final byte[] key) {
    this(key, 8);
  }

  public RowKey(final byte[] key, final int numOfKeyParts) {
    this.key = key;
    this.index = new IntArray(numOfKeyParts);
    for (int offset = 0; offset < key.length; ) {
      final int separator = BytesUtil.indexOf(key, offset, ZERO);
      if (separator < 0) {
        index.add(offset);
        index.add(key.length - offset);
        break;
      }

      index.add(offset);
      index.add(separator - offset);
      offset = separator + ZERO.length;
    }
  }

  public ByteArraySlice get(final int partIndex) {
    final int indexPos = (partIndex << 1);
    return decode(key, index.get(indexPos), index.get(indexPos + 1));
  }

  public boolean getBool(final int partIndex) {
    return getInt8(partIndex) != 0;
  }

  public int getInt8(final int partIndex) {
    return get(partIndex).get(0);
  }

  public int getInt16(final int partIndex) {
    return getInt(partIndex, 2);
  }

  public int getInt24(final int partIndex) {
    return getInt(partIndex, 3);
  }

  public int getInt32(final int partIndex) {
    return getInt(partIndex, 4);
  }

  public long getInt40(final int partIndex) {
    return getLong(partIndex, 5);
  }

  public long getInt48(final int partIndex) {
    return getLong(partIndex, 6);
  }

  public long getInt56(final int partIndex) {
    return getLong(partIndex, 7);
  }

  public long getInt64(final int partIndex) {
    return getLong(partIndex, 8);
  }

  public int getInt(final int partIndex, final int bytesWidth) {
    return Math.toIntExact(getLong(partIndex, bytesWidth));
  }

  public long getLong(final int partIndex, final int bytesWidth) {
    final ByteArraySlice part = get(partIndex);
    int off = 0;
    long result = 0;
    switch (bytesWidth) {
      case 8: result  = (((long)part.get(off++) & 0xff) << 56);
      case 7: result += (((long)part.get(off++) & 0xff) << 48);
      case 6: result += (((long)part.get(off++) & 0xff) << 40);
      case 5: result += (((long)part.get(off++) & 0xff) << 32);
      case 4: result += (((long)part.get(off++) & 0xff) << 24);
      case 3: result += (((long)part.get(off++) & 0xff) << 16);
      case 2: result += (((long)part.get(off++) & 0xff) <<  8);
      case 1: result += (((long)part.get(off) & 0xff));
    }
    return result;
  }

  public long getLong(final int partIndex) {
    final LongValue result = new LongValue();
    VarInt.read(get(partIndex), result);
    return result.get();
  }

  public int getInt(final int partIndex) {
    return Math.toIntExact(getLong(partIndex));
  }

  public String getString(final int partIndex) {
    final ByteArraySlice part = get(partIndex);
    return new String(part.rawBuffer(), part.offset(), part.length(), StandardCharsets.UTF_8);
  }

  // ================================================================================
  //  Decode Key helpers
  // ================================================================================
  public static List<ByteArraySlice> decodeKey(final byte[] key) {
    final ArrayList<ByteArraySlice> parts = new ArrayList<>();
    decodeKey(key, parts::add);
    return parts;
  }

  public static void decodeKey(final byte[] key, final Consumer<ByteArraySlice> consumer) {
    for (int offset = 0; offset < key.length; ) {
      final int separator = BytesUtil.indexOf(key, offset, ZERO);
      if (separator < 0) {
        consumer.accept(decode(key, offset, key.length - offset));
        break;
      }

      //System.out.println("offset -> " + offset + " -> " + (separator - offset));
      consumer.accept(decode(key, offset, separator - offset));
      offset = separator + ZERO.length;
    }
  }

  private static ByteArraySlice decode(final byte[] buf, final int off, final int len) {
    if (BytesUtil.indexOf(buf, off, len, (byte) 0) < 0) {
      return new ByteArraySlice(buf, off, len);
    }

    final ByteArray key = new ByteArray(len - 1);
    for (int i = 0; i < len; ++i) {
      final int index = off + i;
      key.add(buf[index]);
      if (buf[index] == 0) {
        if (buf[index + 1] != 1) {
          throw new IllegalArgumentException("expected 0x01 after 0x00 got [" + (i + 1) + "] = " + Integer.toHexString(buf[off + i + 1]));
        }
        ++i;
      }
    }
    return key.slice();
  }

  public static ByteArraySlice keyWithoutLastComponent(final byte[] key) {
    //final int offset = BytesUtil.lastIndexOf(key, 0, key.length, ZERO);
    final int offset = lastKeyComponentOffset(key);
    return new ByteArraySlice(key, 0, offset - ZERO.length);
  }

  public static ByteArraySlice lastKeyComponent(final byte[] key) {
    final int offset = lastKeyComponentOffset(key);
    return decode(key, offset, key.length - offset);
  }

  private static int lastKeyComponentOffset(final byte[] key) {
    int offset = 0;
    while (offset < key.length) {
      final int separator = BytesUtil.indexOf(key, offset, ZERO);
      if (separator < 0) break;

      offset = separator + ZERO.length;
    }
    return offset;
  }

  public static int nextKeyComponent(final byte[] key, final int offset) {
    return BytesUtil.indexOf(key, offset, ZERO) + ZERO.length;
  }

  // ================================================================================
  //  Key Builder related
  // ================================================================================
  public static RowKeyBuilder newKeyBuilder() {
    return new RowKeyBuilder();
  }

  public static RowKeyBuilder newKeyBuilder(final byte[] key) {
    return new RowKeyBuilder(key);
  }

  public static final class RowKeyBuilder {
    private final ByteArray key = new ByteArray(32);

    private RowKeyBuilder() {
      // no-op
    }

    private RowKeyBuilder(final byte[] key) {
      this.key.add(key);
    }

    public RowKeyBuilder addKeySeparator() {
      key.add(ZERO);
      return this;
    }

    public RowKeyBuilder addBool(final boolean value) {
      return addInt(value ? 1 : 0, 1);
    }

    public RowKeyBuilder addInt8(final int value) {
      return addInt(value & 0xff, 1);
    }

    public RowKeyBuilder addInt16(final int value) {
      return addInt(value, 2);
    }

    public RowKeyBuilder addInt24(final long value) {
      return addInt(value, 3);
    }

    public RowKeyBuilder addInt32(final long value) {
      return addInt(value, 4);
    }

    public RowKeyBuilder addInt40(final long value) {
      return addInt(value, 5);
    }

    public RowKeyBuilder addInt48(final long value) {
      return addInt(value, 6);
    }

    public RowKeyBuilder addInt56(final long value) {
      return addInt(value, 7);
    }

    public RowKeyBuilder addInt64(final long value) {
      return addInt(value, 8);
    }

    public RowKeyBuilder addInt(final long value, final int bytesWidth) {
      final byte[] buf = new byte[bytesWidth];
      IntEncoder.BIG_ENDIAN.writeFixed(buf, 0, value, bytesWidth);
      return add(buf, 0, bytesWidth);
    }

    public RowKeyBuilder addInt(final long value) {
      final byte[] buf = new byte[9];
      final int n = VarInt.write(buf, value);
      return add(buf, 0, n);
    }

    public RowKeyBuilder add(final String value) {
      return add(value.getBytes(StandardCharsets.UTF_8));
    }

    public RowKeyBuilder add(final byte[] buf) {
      return add(buf, 0, buf.length);
    }

    public RowKeyBuilder add(final byte[] buf, final int off) {
      return add(buf, off, buf.length - off);
    }

    public RowKeyBuilder add(final byte[] buf, final int off, final int len) {
      if (key.isNotEmpty()) key.add(ZERO);

      for (int i = 0; i < len; ++i) {
        final byte currentByte = buf[off + i];
        key.add(currentByte);
        if (currentByte == 0x00) {
          // replace 0x00 with 0x0001, 0x0000 is our key separator
          key.add(0x01);
        }
      }
      return this;
    }

    public RowKeyBuilder add(final ByteArraySlice slice) {
      return add(slice.rawBuffer(), slice.offset(), slice.length());
    }

    public byte[] drain() {
      return key.drain();
    }

    public ByteArraySlice slice() {
      return new ByteArraySlice(key.rawBuffer(), 0, key.size());
    }
  }
}
