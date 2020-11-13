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

package tech.dnaco.logging;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import tech.dnaco.bytes.encoding.IntDecoder;
import tech.dnaco.bytes.encoding.IntEncoder;
import tech.dnaco.collections.paged.PagedByteArray;
import tech.dnaco.strings.StringUtil;

final class LogSerde {
  private LogSerde() {
    // no-op
  }

  public static void writeVarInt(final PagedByteArray buffer, final long value) {
    final byte[] buf8 = new byte[9];
    final int vlen = IntEncoder.LITTLE_ENDIAN.writeUnsignedVarLong(buf8, 0, value);
    buffer.add(buf8, 0, vlen);
  }

  public static void writeString(final PagedByteArray buffer, final String value) {
    if (StringUtil.isEmpty(value)) {
      buffer.add(0);
    } else {
      final byte[] blob = value.getBytes(StandardCharsets.UTF_8);
      writeVarInt(buffer, blob.length);
      buffer.add(blob);
    }
  }

  public static void writeString(final OutputStream stream, final String name) throws IOException {
    if (StringUtil.isEmpty(name)) {
      stream.write(0);
    } else {
      final byte[] blob = name.getBytes(StandardCharsets.UTF_8);
      IntEncoder.LITTLE_ENDIAN.writeUnsignedVarLong(stream, blob.length);
      stream.write(blob);
    }
  }

  public static String readString(final InputStream stream) throws IOException {
    final int len = IntDecoder.LITTLE_ENDIAN.readUnsignedVarInt(stream);
    if (len == 0) return "";

    final byte[] blob = stream.readNBytes(len);
    return new String(blob, StandardCharsets.UTF_8);
  }

  public static void writeBlob(final OutputStream stream, final byte[] buf, final int off, final int len) throws IOException {
    if (len == 0) {
      stream.write(0);
    } else {
      IntEncoder.LITTLE_ENDIAN.writeUnsignedVarLong(stream, len);
      stream.write(buf, off, len);
    }
  }

  public static void writeBlob8(final PagedByteArray buffer, final String value) {
    final byte[] blob = value.getBytes(StandardCharsets.UTF_8);
    writeBlob8(buffer, blob, 0, blob.length);
  }

  public static void writeBlob8(final PagedByteArray buffer, final byte[] buf, final int off, final int len) {
    if (len == 0) {
      buffer.add(0);
    } else {
      buffer.add(len);
      buffer.add(buf, off, len);
    }
  }

  public static void writeBlob8(final OutputStream stream, final byte[] buf, final int off, final int len) throws IOException {
    if (len == 0) {
      stream.write(0);
    } else {
      stream.write(len);
      stream.write(buf, off, len);
    }
  }

  public static byte[] readBlob8(final PagedByteArray buffer, final int offset) {
    final int len = buffer.get(offset);
    if (len == 0) return null;

    final byte[] module = new byte[len];
    buffer.get(offset + 1, module, 0, len);
    return module;
  }

  public static byte[] readBlob8(final InputStream stream) throws IOException {
    final int len = stream.read() & 0xff;
    if (len == 0) return null;

    return stream.readNBytes(len);
  }
}
