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

package tech.dnaco.bytes.encoding;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.collections.LongValue;
import tech.dnaco.collections.arrays.ByteArray;

public final class RowKeyUtil {
  private static final byte[] ZERO = new byte[] { 0, 0 };

  private RowKeyUtil() {
    // no-op
  }

  public static ByteArraySlice keyWithoutLastComponent(final byte[] key) {
    //final int offset = BytesUtil.lastIndexOf(key, 0, key.length, ZERO);
    final int offset = lastKeyComponentOffset(key);
    return new ByteArraySlice(key, 0, offset - ZERO.length);
  }

  public static byte[] lastKeyComponent(final byte[] key) {
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

  public static List<byte[]> decodeKey(final byte[] key) {
    final ArrayList<byte[]> parts = new ArrayList<>();
    decodeKey(key, parts::add);
    return parts;
  }

  public static void decodeKey(final byte[] key, final Consumer<byte[]> consumer) {
    int offset = 0;
    while (offset < key.length) {
      final int separator = BytesUtil.indexOf(key, offset, ZERO);
      if (separator < 0) {
        consumer.accept(decode(key, offset, key.length - offset));
        break;
      }

      consumer.accept(decode(key, offset, separator - offset));
      offset = separator + ZERO.length;
    }
  }

  private static byte[] decode(final byte[] buf, final int off, final int len) {
    if (BytesUtil.indexOf(buf, off, len, (byte) 0) < 0) {
      return Arrays.copyOfRange(buf, off, off + len);
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
    return key.drain();
  }

  public static RowKeyBuilder newKeyBuilder() {
    return new RowKeyBuilder();
  }

  public static RowKeyBuilder newKeyBuilder(final byte[] key) {
    return new RowKeyBuilder(key);
  }

  public static RowKeyDecoder newKeyDecoder(final byte[] key) {
    return new RowKeyDecoder(key);
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

    public RowKeyBuilder addInt8(final int value) {
      addInt(value & 0xff, 1);
      return this;
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

    public byte[] drain() {
      return key.drain();
    }

    public ByteArraySlice slice() {
      return new ByteArraySlice(key.rawBuffer(), 0, key.size());
    }

    public RowKeyBuilder dump() {
      System.out.println(key);
      return this;
    }
  }

  public static final class RowKeyDecoder {
    private final byte[] key;
    private int offset;

    private RowKeyDecoder(final byte[] key) {
      this.key = key;
      this.offset = 0;
    }

    public int getInt8() {
      return (int) getInt(1);
    }

    public int getInt16() {
      return (int) getInt(2);
    }

    public int getInt24() {
      return (int) getInt(3);
    }

    public long getInt32() {
      return getInt(4);
    }

    public long addInt40() {
      return getInt(5);
    }

    public long addInt48() {
      return getInt(6);
    }

    public long addInt56() {
      return getInt(7);
    }

    public long addInt64() {
      return getInt(8);
    }

    public long getInt(final int bytesWidth) {
      final long value = IntDecoder.BIG_ENDIAN.readFixed(key, offset, bytesWidth);
      this.offset = nextKeyComponent(key, offset);
      return value;
    }

    public long getInt() {
      final LongValue result = new LongValue();
      final int n = VarInt.read(key, offset, key.length - offset, result);
      this.offset = nextKeyComponent(key, offset + n);
      return result.get();
    }

    public String getString() {
      final int nextOffset = nextKeyComponent(key, offset);
      final String value = new String(key, offset, nextOffset - ZERO.length);
      this.offset = nextOffset;
      return value;
    }

    public ByteArraySlice getBytes() {
      final int nextOffset = nextKeyComponent(key, offset);
      final ByteArraySlice value = new ByteArraySlice(key, offset, nextOffset - ZERO.length);
      this.offset = nextOffset;
      return value;
    }
  }
}
