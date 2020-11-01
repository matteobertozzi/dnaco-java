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

package tech.dnaco.io;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;

import tech.dnaco.collections.ArrayUtil;
import tech.dnaco.logging.Logger;

public final class IOUtil {
  private IOUtil() {
    // no-op
  }

  public static void closeQuietly(final Closeable closeable) {
    try {
      closeable.close();
    } catch (IOException e) {
      Logger.trace("unable to close stream: {}", closeable);
    }
  }

  public static void closeQuietly(final AutoCloseable closeable) {
    try {
      closeable.close();
    } catch (Exception e) {
      Logger.trace("unable to close stream: {}", closeable);
    }
  }

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

  public static void write(OutputStream stream, MessageDigest digest, final byte[] buf) throws IOException {
    write(stream, digest, buf, 0, buf.length);
  }

  public static void write(OutputStream stream, MessageDigest digest, final byte[] buf, final int off, final int len) throws IOException {
    stream.write(buf, off, len);
    digest.update(buf, off, len);
  }
}