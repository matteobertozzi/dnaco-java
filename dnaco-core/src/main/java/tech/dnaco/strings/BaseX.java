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

package tech.dnaco.strings;

import java.util.Arrays;
import java.util.Base64;
import java.util.function.Function;

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.bytes.encoding.IntEncoder;
import tech.dnaco.collections.arrays.ByteArray;
import tech.dnaco.util.RandData;

public class BaseX {
  // ===============================================================================================
  //  Encode Base32
  // ===============================================================================================
  //         1         2         3         4         5         6         7         8
  // 0 1 2 3 4 0 1 2 3 4 0 1 2 3 4 0 1 2 3 4 0 1 2 3 4 0 1 2 3 4 0 1 2 3 4 0 1 2 3 4
  // 7 6 5 4 3 2 1 0 7 6 5 4 3 2 1 0 7 6 5 4 3 2 1 0 7 6 5 4 3 2 1 0 7 6 5 4 3 2 1 0
  //               1               2               3               4               5
  private static final char[] ALPHABET_32 = "0123456789ABCDEFGHIJKLMNOPQRSTUV".toCharArray();
  private static final int[] DECODE_32 = new int[128];
  static {
    Arrays.fill(DECODE_32, -1);
    for (int i = 0; i < ALPHABET_32.length; ++i) {
      DECODE_32[ALPHABET_32[i]] = i;
    }
  }

  private static int lengthDecoded32(final int encodedLength) {
    final int length = (encodedLength / 8) * 5;
    switch (encodedLength % 8) {
      case 7: return length + 4;
      case 5: return length + 3;
      case 4: return length + 2;
      case 2: return length + 1;
    }
    return length;
  }

  private static int lengthEncoded32(final int decodedLength) {
    final int length = (decodedLength / 5) * 8;
    switch (decodedLength % 5) {
      case 4: return length + 7;
      case 3: return length + 5;
      case 2: return length + 4;
      case 1: return length + 2;
    }
    return length;
  }

  public static String encode32(final byte[] data) {
    return encode32(data, 0, BytesUtil.length(data));
  }

  public static String encode32(final byte[] data, final int offset, int length) {
    if (length == 0) return "";

    final StringBuilder builder = new StringBuilder(lengthEncoded32(length));

    int i = offset;
    while (length >= 5) {
      final long x = ((long)(data[i] & 0xff) << 32)
                    | ((long)(data[i + 1] & 0xff) << 24)
                    | (data[i + 2] & 0xff) << 16
                    | (data[i + 3] & 0xff) << 8
                    | (data[i + 4] & 0xff);
      builder.append(ALPHABET_32[(int)((x >> 35) & 0x1f)]);
      builder.append(ALPHABET_32[(int)((x >> 30) & 0x1f)]);
      builder.append(ALPHABET_32[(int)((x >> 25) & 0x1f)]);
      builder.append(ALPHABET_32[(int)((x >> 20) & 0x1f)]);
      builder.append(ALPHABET_32[(int)((x >> 15) & 0x1f)]);
      builder.append(ALPHABET_32[(int)((x >> 10) & 0x1f)]);
      builder.append(ALPHABET_32[(int)((x >>  5) & 0x1f)]);
      builder.append(ALPHABET_32[(int)((x) & 0x1f)]);
      length -= 5;
      i += 5;
    }

    switch (length) {
      case 4: // 7byte
        final long x4 = ((long)(data[i] & 0xff) << 27)
                      | (data[i + 1] & 0xff) << 19
                      | (data[i + 2] & 0xff) << 11
                      | (data[i + 3] & 0xff) << 3;
        builder.append(ALPHABET_32[(int)((x4 >> 30) & 0x1f)]);
        builder.append(ALPHABET_32[(int)((x4 >> 25) & 0x1f)]);
        builder.append(ALPHABET_32[(int)((x4 >> 20) & 0x1f)]);
        builder.append(ALPHABET_32[(int)((x4 >> 15) & 0x1f)]);
        builder.append(ALPHABET_32[(int)((x4 >> 10) & 0x1f)]);
        builder.append(ALPHABET_32[(int)((x4 >>  5) & 0x1f)]);
        builder.append(ALPHABET_32[(int)((x4) & 0x1f)]);
        break;
      case 3: // 5byte
        final int x3 =  (data[i] & 0xff) << 17 |
                        (data[i + 1] & 0xff) << 9  |
                        (data[i + 2] & 0xff) << 1;
        builder.append(ALPHABET_32[(x3 >> 20) & 0x1f]);
        builder.append(ALPHABET_32[(x3 >> 15) & 0x1f]);
        builder.append(ALPHABET_32[(x3 >> 10) & 0x1f]);
        builder.append(ALPHABET_32[(x3 >>  5) & 0x1f]);
        builder.append(ALPHABET_32[(x3) & 0x1f]);
        break;
      case 2: // 4byte
        final int x2 = ((data[i] & 0xff) << 12) | ((data[i + 1] & 0xff) << 4);
        builder.append(ALPHABET_32[(x2 >> 15) & 0x1f]);
        builder.append(ALPHABET_32[(x2 >> 10) & 0x1f]);
        builder.append(ALPHABET_32[(x2 >>  5) & 0x1f]);
        builder.append(ALPHABET_32[(x2) & 0x1f]);
        break;
      case 1: // 2byte
        final int x1 = (data[i] & 0xff) << 2;
        builder.append(ALPHABET_32[(x1 >>  5) & 0x1f]);
        builder.append(ALPHABET_32[(x1) & 0x1f]);
        break;
    }

    return builder.toString();
  }

  public static byte[] decode32(final byte[] encoded) {
    return decode32(encoded, 0, BytesUtil.length(encoded));
  }

  public static byte[] decode32(final byte[] encoded, final int offset, int length) {
    if (length == 0) return null;

    final byte[] data = new byte[lengthDecoded32(length)];
    int dataIndex = 0;

    int i = offset;
    while (length >= 8) {
      final long x5 = ((long)(DECODE_32[encoded[i] & 0xff]) << 35)
                    | ((long)(DECODE_32[encoded[i + 1] & 0xff]) << 30)
                    | ((long)DECODE_32[encoded[i + 2] & 0xff]) << 25
                    | (DECODE_32[encoded[i + 3] & 0xff] << 20)
                    | (DECODE_32[encoded[i + 4] & 0xff] << 15)
                    | (DECODE_32[encoded[i + 5] & 0xff] << 10)
                    | (DECODE_32[encoded[i + 6] & 0xff] <<  5)
                    | (DECODE_32[encoded[i + 7] & 0xff]);
          data[dataIndex++] = (byte)((x5 >> 32) & 0xff);
          data[dataIndex++] = (byte)((x5 >> 24) & 0xff);
          data[dataIndex++] = (byte)((x5 >> 16) & 0xff);
          data[dataIndex++] = (byte)((x5 >> 8) & 0xff);
          data[dataIndex++] = (byte)((x5) & 0xff);

      i += 8;
      length -= 8;
    }

    switch (length) {
      case 7:
        final long x4 = ((long)(DECODE_32[encoded[i] & 0xff]) << 30)
                      | ((long)(DECODE_32[encoded[i + 1] & 0xff]) << 25)
                      | (DECODE_32[encoded[i + 2] & 0xff] << 20)
                      | (DECODE_32[encoded[i + 3] & 0xff] << 15)
                      | (DECODE_32[encoded[i + 4] & 0xff] << 10)
                      | (DECODE_32[encoded[i + 5] & 0xff] <<  5)
                      | (DECODE_32[encoded[i + 6] & 0xff]);
        data[dataIndex++] = (byte)((x4 >> 27) & 0xff);
        data[dataIndex++] = (byte)((x4 >> 19) & 0xff);
        data[dataIndex++] = (byte)((x4 >> 11) & 0xff);
        data[dataIndex++] = (byte)((x4 >> 3) & 0xff);
        break;
      case 5:
        final int x3 = (DECODE_32[encoded[i] & 0xff] << 20)
                     | (DECODE_32[encoded[i + 1] & 0xff] << 15)
                     | (DECODE_32[encoded[i + 2] & 0xff] << 10)
                     | (DECODE_32[encoded[i + 3] & 0xff] <<  5)
                     | (DECODE_32[encoded[i + 4] & 0xff]);
        data[dataIndex++] = (byte)((x3 >> 17) & 0xff);
        data[dataIndex++] = (byte)((x3 >> 9) & 0xff);
        data[dataIndex++] = (byte)((x3 >> 1) & 0xff);
        break;
      case 4:
        final int x2 = (DECODE_32[encoded[i] & 0xff] << 15)
                     | (DECODE_32[encoded[i + 1] & 0xff] << 10)
                     | (DECODE_32[encoded[i + 2] & 0xff] <<  5)
                     | (DECODE_32[encoded[i + 3] & 0xff]);
        data[dataIndex++] = (byte)((x2 >> 12) & 0xff);
        data[dataIndex++] = (byte)((x2 >> 4) & 0xff);
        break;
      case 2:
        final int x1 = (DECODE_32[encoded[i] & 0xff] << 5)
                     | (DECODE_32[encoded[i + 1] & 0xff]);
        data[dataIndex++] = (byte)((x1 >> 2) & 0xff);
        break;
    }

    return data;
  }

  public static byte[] decode32(final String encoded) {
    return decode32(encoded, 0, StringUtil.length(encoded));
  }

  public static byte[] decode32(final String encoded, final int offset, int length) {
    if (StringUtil.isEmpty(encoded)) return null;

    final byte[] data = new byte[lengthDecoded32(length)];
    int dataIndex = 0;

    int i = offset;
    while (length >= 8) {
      final long x5 = ((long)(DECODE_32[encoded.charAt(i) & 0xff]) << 35)
                    | ((long)(DECODE_32[encoded.charAt(i + 1) & 0xff]) << 30)
                    | ((long)DECODE_32[encoded.charAt(i + 2) & 0xff]) << 25
                    | (DECODE_32[encoded.charAt(i + 3) & 0xff] << 20)
                    | (DECODE_32[encoded.charAt(i + 4) & 0xff] << 15)
                    | (DECODE_32[encoded.charAt(i + 5) & 0xff] << 10)
                    | (DECODE_32[encoded.charAt(i + 6) & 0xff] <<  5)
                    | (DECODE_32[encoded.charAt(i + 7) & 0xff]);
          data[dataIndex++] = (byte)((x5 >> 32) & 0xff);
          data[dataIndex++] = (byte)((x5 >> 24) & 0xff);
          data[dataIndex++] = (byte)((x5 >> 16) & 0xff);
          data[dataIndex++] = (byte)((x5 >> 8) & 0xff);
          data[dataIndex++] = (byte)((x5) & 0xff);

      i += 8;
      length -= 8;
    }

    switch (length) {
      case 7:
        final long x4 = ((long)(DECODE_32[encoded.charAt(i) & 0xff]) << 30)
                      | ((long)(DECODE_32[encoded.charAt(i + 1) & 0xff]) << 25)
                      | (DECODE_32[encoded.charAt(i + 2) & 0xff] << 20)
                      | (DECODE_32[encoded.charAt(i + 3) & 0xff] << 15)
                      | (DECODE_32[encoded.charAt(i + 4) & 0xff] << 10)
                      | (DECODE_32[encoded.charAt(i + 5) & 0xff] <<  5)
                      | (DECODE_32[encoded.charAt(i + 6) & 0xff]);
        data[dataIndex++] = (byte)((x4 >> 27) & 0xff);
        data[dataIndex++] = (byte)((x4 >> 19) & 0xff);
        data[dataIndex++] = (byte)((x4 >> 11) & 0xff);
        data[dataIndex++] = (byte)((x4 >> 3) & 0xff);
        break;
      case 5:
        final int x3 = (DECODE_32[encoded.charAt(i) & 0xff] << 20)
                     | (DECODE_32[encoded.charAt(i + 1) & 0xff] << 15)
                     | (DECODE_32[encoded.charAt(i + 2) & 0xff] << 10)
                     | (DECODE_32[encoded.charAt(i + 3) & 0xff] <<  5)
                     | (DECODE_32[encoded.charAt(i + 4) & 0xff]);
        data[dataIndex++] = (byte)((x3 >> 17) & 0xff);
        data[dataIndex++] = (byte)((x3 >> 9) & 0xff);
        data[dataIndex++] = (byte)((x3 >> 1) & 0xff);
        break;
      case 4:
        final int x2 = (DECODE_32[encoded.charAt(i) & 0xff] << 15)
                     | (DECODE_32[encoded.charAt(i + 1) & 0xff] << 10)
                     | (DECODE_32[encoded.charAt(i + 2) & 0xff] <<  5)
                     | (DECODE_32[encoded.charAt(i + 3) & 0xff]);
        data[dataIndex++] = (byte)((x2 >> 12) & 0xff);
        data[dataIndex++] = (byte)((x2 >> 4) & 0xff);
        break;
      case 2:
        final int x1 = (DECODE_32[encoded.charAt(i) & 0xff] << 5)
                     | (DECODE_32[encoded.charAt(i + 1) & 0xff]);
        data[dataIndex++] = (byte)((x1 >> 2) & 0xff);
        break;
    }

    return data;
  }

  // ===============================================================================================
  //  Encode Base64
  // ===============================================================================================
  //           1           2           3           4
  // 0 1 2 3 4 5 0 1 2 3 4 5 0 1 2 3 4 5 0 1 2 3 4 5
  // 7 6 5 4 3 2 1 0 7 6 5 4 3 2 1 0 7 6 5 4 3 2 1 0
  //               1               2               3
  private static final char[] ALPHABET_64 = "-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz".toCharArray();
  private static final int[] DECODE_64 = new int[128];
  static {
    Arrays.fill(DECODE_64, -1);
    for (int i = 0; i < ALPHABET_64.length; ++i) {
      DECODE_64[ALPHABET_64[i]] = i;
    }
  }

  public static String encode64(final byte[] value) {
    return encode64(value, 0, BytesUtil.length(value));
  }

  public static String encode64(final byte[] value, final int off, final int length) {
    final StringBuilder result = new StringBuilder(length * 2);
    int offset = off;
    while ((offset + 3) <= length) {
      final int v0 = value[offset++] & 0xff;
      final int v1 = value[offset++] & 0xff;
      final int v2 = value[offset++] & 0xff;

      result.append(ALPHABET_64[v0 >>> 2]);
      result.append(ALPHABET_64[((v0 & 3) << 4) | (v1 >>> 4)]);
      result.append(ALPHABET_64[((v1 & 0xf) << 2) | (v2 >>> 6)]);
      result.append(ALPHABET_64[v2 & 0x3f]);
    }

    switch (length - offset) {
      case 1: {
        final int v0 = value[offset++] & 0xff;
        result.append(ALPHABET_64[v0 >>> 2]);
        result.append(ALPHABET_64[(v0 & 3) << 4]);
        break;
      }
      case 2: {
        final int v0 = value[offset++] & 0xff;
        final int v1 = value[offset++] & 0xff;
        result.append(ALPHABET_64[v0 >>> 2]);
        result.append(ALPHABET_64[((v0 & 3) << 4) | (v1 >>> 4)]);
        result.append(ALPHABET_64[(v1 & 0xf) << 2]);
        break;
      }
    }
    return result.toString();
  }

  public static byte[] decode64(final String data) {
    return decode64(data, 0, StringUtil.length(data));
  }

  public static byte[] decode64(final String data, final int offset, final int length) {
    if (length == 0) return BytesUtil.EMPTY_BYTES;

    final byte[] buffer = new byte[lengthDecoded64(length)];
    int i = offset;
    int index = 0;
    while ((i + 4) <= length) {
      final int v0 = DECODE_64[data.charAt(i++)];
      final int v1 = DECODE_64[data.charAt(i++)];
      final int v2 = DECODE_64[data.charAt(i++)];
      final int v3 = DECODE_64[data.charAt(i++)];
      if ((v0 | v1 | v2 | v3) < 0) {
        throw new IllegalArgumentException("invalid encoded data: " + data);
      }
      buffer[index++] = (byte)((v0 << 2) | v1 >> 4);
      buffer[index++] = (byte)((v1 << 4) | v2 >> 2);
      buffer[index++] = (byte)((v2 << 6) | v3);
    }

    switch (length - i) {
      case 2: {
        final int v0 = DECODE_64[data.charAt(i++)];
        final int v1 = DECODE_64[data.charAt(i++)];
        if ((v0 | v1) < 0) {
          throw new IllegalArgumentException("invalid encoded data: " + data);
        }
        buffer[index++] = (byte)((v0 << 2) | v1 >> 4);
        break;
      }
      case 3: {
        final int v0 = DECODE_64[data.charAt(i++)];
        final int v1 = DECODE_64[data.charAt(i++)];
        final int v2 = DECODE_64[data.charAt(i++)];
        if ((v0 | v1 | v2) < 0) {
          throw new IllegalArgumentException("invalid encoded data: " + data);
        }
        buffer[index++] = (byte)((v0 << 2) | v1 >> 4);
        buffer[index++] = (byte)((v1 << 4) | v2 >> 2);
        break;
      }
    }
    return buffer;
  }

  private static int lengthDecoded64(final int encodedLength) {
    final int length = (encodedLength / 4) * 3;
    switch (encodedLength & 3) {
      case 2: return length + 1;
      case 3: return length + 2;
    }
    return length;
  }


  public record Data (byte[] value, String encoded) {}
  private static void testEncodeDecode(final Function<byte[], String> encode, final Function<String, byte[]> decode) {
    final Data[] data = new Data[1000];
    for (int i = 0; i < data.length; ++i) {
      final byte[] v;
      if (true) {
        v = RandData.generateBytes(i & 63);
      } else {
        v = new byte[8];
        IntEncoder.BIG_ENDIAN.writeFixed64(v, 0, i);
      }
      data[i] = new Data(v, encode.apply(v));
    }
    Arrays.sort(data, (a, b) -> BytesUtil.compare(a.value(), b.value()));

    for (int i = 1; i < data.length; ++i) {
      //System.out.println(data[i].encoded() + " -> " + Arrays.toString(data[i].value()));
      if (data[i].encoded().compareTo(data[i - 1].encoded()) < 0) {
        throw new IllegalArgumentException(data[i] + " " + data[i - 1]);
      }
    }

    for (int i = 0; i < data.length; ++i) {
      if (!BytesUtil.equals(data[i].value(), decode.apply(data[i].encoded()))) {
        throw new IllegalArgumentException("invalid decode");
      }
    }
  }

  public static void main(final String[] args) {
    //testEncodeDecode(BaseX::encode32, BaseX::decode32);
    //testEncodeDecode(BaseX::encode64, BaseX::decode64);
    System.out.println(encode32(new byte[] { 1, 2, 3, 4, 5 }));

    final ByteArray buffer = new ByteArray(15);
    for (int i = 0; i < 32; ++i) {
      buffer.add(i & 0xff);
      final String x = Base64.getEncoder().encodeToString(buffer.buffer());
      System.out.println(x.length() + " -> " + Math.ceil((buffer.size() * 8) / 6.0) + " -> " + x);
    }
  }
}