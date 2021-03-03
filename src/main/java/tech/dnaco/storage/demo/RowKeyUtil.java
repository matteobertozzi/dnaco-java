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

package tech.dnaco.storage.demo;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.collections.arrays.ByteArray;

// https://sqlite.org/src4/file?name=src/varint.c&ci=trunk
public final class RowKeyUtil {
  private static final byte[] ZERO_ESCAPE = new byte[] { 0, 1 };
  private static final byte[] ZERO = new byte[] { 0, 0 };

  private RowKeyUtil() {
    // no-op
  }

  public static void main(final String[] args) {
    final byte[] key = newKeyBuilder().add("aaa").add("bbb").add("ccc").drain();
    final byte[] component = lastKeyComponent(key);
    System.out.println(component.length + " -> " + new String(component));
  }

  public static ByteArraySlice keyWithoutLastComponent(final byte[] key) {
    final int offset = BytesUtil.lastIndexOf(key, 0, key.length, ZERO);
    return new ByteArraySlice(key, 0, offset);
  }

  public static byte[] lastKeyComponent(final byte[] key) {
    final int offset = BytesUtil.lastIndexOf(key, 0, key.length, ZERO) + ZERO.length;
    return decode(key, offset, key.length - offset);
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

    System.out.println(BytesUtil.toString(buf, off, len));
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

  public static final class RowKeyBuilder {
    private final ByteArray key = new ByteArray(32);

    public RowKeyBuilder() {
      // no-op
    }

    public RowKeyBuilder(final byte[] key) {
      this.key.add(key);
    }

    public RowKeyBuilder addKeySeparator() {
      key.add(ZERO);
      return this;
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
      System.out.println(key.toString());
      return this;
    }
  }
}
