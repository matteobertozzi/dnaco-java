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

package tech.dnaco.io;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.bytes.encoding.IntDecoder;
import tech.dnaco.bytes.encoding.IntEncoder;
import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.logging.Logger;
import tech.dnaco.strings.StringUtil;

public final class IOUtil {
  private IOUtil() {
    // no-op
  }

  // ===============================================================================================
  //  Close related
  // ===============================================================================================
  public static void closeQuietly(final Closeable closeable) {
    try {
      closeable.close();
    } catch (final IOException e) {
      Logger.trace("unable to close stream: {}", closeable);
    }
  }

  public static void closeQuietly(final AutoCloseable closeable) {
    try {
      closeable.close();
    } catch (final Exception e) {
      Logger.trace("unable to close stream: {}", closeable);
    }
  }

  // ===============================================================================================
  //  Read related
  // ===============================================================================================
  public static byte[] readNBytes(final InputStream stream, final int len) throws IOException {
    final byte[] buf = stream.readNBytes(len);
    if (buf == null || buf.length != len) {
      throw new EOFException("unable to read " + len + ", got only " + ArrayUtil.length(buf));
    }
    return buf;
  }

  public static void readNBytes(final InputStream stream, final byte[] buf, final int off, final int len) throws IOException {
    final int n = stream.readNBytes(buf, off, len);
    if (n != len) {
      throw new EOFException("unable to read " + len + ", got only " + n);
    }
  }

  // ===============================================================================================
  //  Read Types
  // ===============================================================================================
  public static byte[] readBlob8(final InputStream stream) throws IOException {
    final int len = stream.read() & 0xff;
    if (len == 0) return BytesUtil.EMPTY_BYTES;

    return readNBytes(stream, len);
  }

  public static String readString(final InputStream stream) throws IOException {
    final int len = IntDecoder.readUnsignedVarInt(stream);
    if (len == 0) return "";

    final byte[] blob = IOUtil.readNBytes(stream, len);
    return new String(blob);
  }

  // ===============================================================================================
  //  Write Types
  // ===============================================================================================
  public static void writeBlob8(final OutputStream stream, final String value) throws IOException {
    writeBlob8(stream, value != null ? value.getBytes() : null);
  }

  public static void writeBlob8(final OutputStream stream, final byte[] value) throws IOException {
    writeBlob8(stream, value, 0, BytesUtil.length(value));
  }

  public static void writeBlob8(final OutputStream stream, final byte[] value, final int off, final int len)
      throws IOException {
    if (len > 255) throw new IllegalArgumentException("value length must fit in 8bit, got " + len);

    stream.write(len);
    if (len > 0) {
      stream.write(value, off, len);
    }
  }

  public static void writeString(final OutputStream stream, final String value) throws IOException {
    if (StringUtil.isEmpty(value)) {
      stream.write(0);
    } else {
      final byte[] blob = value.getBytes();
      IntEncoder.writeUnsignedVarLong(stream, blob.length);
      stream.write(blob);
    }
  }
}
