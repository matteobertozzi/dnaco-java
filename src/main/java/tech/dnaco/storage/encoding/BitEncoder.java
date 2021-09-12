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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.bytes.encoding.IntEncoder;
import tech.dnaco.data.compression.GzipUtil;
import tech.dnaco.data.json.JsonUtil;
import tech.dnaco.strings.BaseN;

public class BitEncoder implements AutoCloseable {
  private final OutputStream stream;
  private final long mask;
  private final int width;

  private long vBuffer = 0;
  private int availBits;
  private int vBits = 0;

  public BitEncoder(final OutputStream stream, final int width) {
    this.stream = stream;
    this.width = width;
    this.mask = (width == 64) ? 0xffffffffffffffffL : ((1L << width) - 1);
    this.availBits = Long.BYTES << 3;
  }

  @Override
  public void close() throws IOException {
    if (availBits != (Long.BYTES << 3)) {
      flush();
    }
  }

  public void add(final int v) throws IOException {
    if (availBits == 0) flush();

    //System.out.println("ADD " + Long.toBinaryString(v) + " MASK " + Long.toBinaryString(mask) + " -> " + Long.toBinaryString(vBuffer));
    if (availBits >= width) {
      vBuffer = (vBuffer << width) | (v & mask);
      availBits -= width;
      vBits += width;
      return;
    }

    final int v1Width = (width - availBits);
    final long maskV0 = (1L << availBits) - 1;
    final long maskV1 = (1L << v1Width) - 1;

    final long v0 = (v >> v1Width) & maskV0;
    final long v1 = v & maskV1;

    //System.out.println("AVAIL-BITS " + availBits);
    //System.out.println(" - v0: " + Long.toBinaryString(v0));
    //System.out.println(" - v1: " + Long.toBinaryString(v1));
    vBuffer = (vBuffer << availBits) | v0;
    vBits += availBits;
    availBits = 0;
    flush();

    vBuffer = v1;
    availBits = (Long.BYTES << 3) - v1Width;
    vBits += v1Width;
  }

  public void flush() throws IOException {
    //System.out.println("ENCODE vBuffer=" + vBuffer + " -> " + Long.toBinaryString(vBuffer) + " bits=" + vBits + " bytes=" + ((vBits + 7) / 8));
    IntEncoder.BIG_ENDIAN.writeFixed(stream, vBuffer, (vBits + 7) >> 3);
    availBits = Long.BYTES << 3;
    vBuffer = 0;
    vBits = 0;
  }

  private static int[] TABLE = new int[128];
  private static char[] CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ_".toCharArray();
  static {
    for (int i = 0; i < TABLE.length; ++i) {
      final int index = Arrays.binarySearch(CHARS, (char)i);
      TABLE[i] = (index < 0) ? -1 : index;
    }
  }

  private static int prefix(final String a, final String b) {
    final int aLen = a.length();
    final int bLen = b.length();
    final int len = Math.min(aLen, bLen);
    for (int i = 0; i < len; ++i) {
      if (a.charAt(i) != b.charAt(i)) {
        return i;
      }
    }
    return len;
  }

  private static List<String> prefix(final String[] source) {
    final ArrayList<String> foo = new ArrayList<>();

    String prev = source[0];
    for (int i = 0; i < source.length; ++i) {
      final int prefix = prefix(prev, source[i]);
      final String r = prefix + ":" + source[i].substring(prefix);
      foo.add(r);
      prev = source[i];
    }
    return foo;
  }

  private static int[] TABLE2 = new int[128];
  private static char[] CHARS2 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ_:1234".toCharArray();
  static {
    for (int i = 0; i < TABLE2.length; ++i) {
      final int index = Arrays.binarySearch(CHARS2, (char)i);
      TABLE2[i] = (index < 0) ? -1 : index;
    }
  }
  private static byte[] prefix2(final String[] source) throws IOException {
    try (ByteArrayOutputStream buf = new ByteArrayOutputStream()) {
      try (BitEncoder encoder = new BitEncoder(buf, 5)) {
        String prev = source[0];
        for (int i = 0; i < source.length; ++i) {
          final int prefix = prefix(prev, source[i]);
          final String r = BaseN.encodeBaseN(prefix, CHARS2) + ":" + source[i].substring(prefix);
          for (int k = 0, n = r.length(); k < n; ++k) {
            encoder.add(TABLE2[r.charAt(k)]);
          }
          prev = source[i];
        }
      }
      return buf.toByteArray();
    }
  }


  public static void main(final String[] args) throws IOException {
    final String[] source = new String[] {
      "API_KEY_CREATE",
      "API_KEY_DELETE",
      "API_KEY_EDIT",
      "API_KEY_INFO",
      "API_KEY_LIST",
      "AUTH_CONFIG",
      "DEVICE_DATASOURCE_MANAGEMENT",
      "DEVICE_DATA_MANAGEMENT",
      "DEVICE_MANAGEMENT",
      "GROUP_API_KEY_MANAGEMENT",
      "GROUP_CREATE",
      "GROUP_DELETE",
      "GROUP_EDIT",
      "GROUP_INFO",
      "GROUP_LIST",
      "GROUP_USER_MANAGEMENT",
      "PERMISSION_API_KEY_INFO",
      "PERMISSION_API_KEY_MANAGEMENT",
      "PERMISSION_AVAILABLE",
      "PERMISSION_GROUP_INFO",
      "PERMISSION_GROUP_MANAGEMENT",
      "PERMISSION_USER_INFO",
      "PERMISSION_USER_MANAGEMENT",
      "USER_ACTIVATION_SEND",
      "USER_ACTIVATION_VERIFY",
      "USER_CHANGE_PASSWORD_VERIFY",
      "USER_CREATE",
      "USER_DELETE",
      "USER_EDIT",
      "USER_INFO",
      "USER_LIST",
      "USER_RESET_PASSWORD_SEND",
      "USER_RESET_PASSWORD_VERIFY",
      "USER_SIGN_IN",
      "USER_SIGN_IN_OAUTH",
      "USER_SIGN_UP"
    };
    Arrays.sort(source);
    final int ref = JsonUtil.toJson(source).length();
    System.out.println(JsonUtil.toJson(prefix2(source)));
    System.out.println("source:  " + ref);
    System.out.println("gzource: " + GzipUtil.compress(JsonUtil.toJson(source)).length);
    System.out.println("gzource: " + JsonUtil.toJson(GzipUtil.compress(JsonUtil.toJson(source))).length());
    System.out.println("prefix:  " + JsonUtil.toJson(prefix(source)).length() + " " + (1.0 - (JsonUtil.toJson(prefix(source)).length() / (double)ref)));
    System.out.println("prefixS: " + JsonUtil.toJson(prefix2(source)).length() + " " + (1.0 - (JsonUtil.toJson(prefix2(source)).length() / (double)ref)));
    System.out.println("prefixB: " + prefix2(source).length + " " + (1.0 - (prefix2(source).length / (double)ref)));
    System.out.println("prefixB: " + GzipUtil.compress(prefix2(source)).length);
    if (true) return;

    System.out.println(Arrays.toString(TABLE));
    final String text = "DATASOURCE_MANAGEMENT";
    try (ByteArrayOutputStream buf = new ByteArrayOutputStream()) {
      try (BitEncoder encoder = new BitEncoder(buf, 5)) {
        for (int i = 0, n = text.length(); i < n; ++i) {
          encoder.add(TABLE[text.charAt(i)]);
        }
      }
      final byte[] buffo = buf.toByteArray();
      System.out.println(text.length() + "/" + buffo.length + " -> " + (buffo.length / (double)text.length()));
      System.out.println(BytesUtil.toBinaryString(buffo));
    }
  }
}
