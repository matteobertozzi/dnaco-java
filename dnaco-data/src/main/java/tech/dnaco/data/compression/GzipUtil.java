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

package tech.dnaco.data.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipUtil {
  private GzipUtil() {
    // no-op
  }

  public static byte[] compress(final String data) throws IOException {
    return compress(data.getBytes());
  }

  public static byte[] compress(final byte[] data) throws IOException {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream(data.length)) {
      try (GZIPOutputStream gz = new GZIPOutputStream(out)) {
        gz.write(data);
      }
      return out.toByteArray();
    }
  }

  public static byte[] compress(final String data, final int level) throws IOException {
    return compress(data.getBytes(), level);
  }

  public static byte[] compress(final byte[] data, final int level) throws IOException {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream(data.length)) {
      try (GZIPOutputStream gz = new GzipOutputStreamWithLevel(out, level)) {
        gz.write(data);
      }
      return out.toByteArray();
    }
  }

  public static byte[] uncompress(final byte[] gzData) throws IOException {
    try (ByteArrayInputStream in = new ByteArrayInputStream(gzData)) {
      try (GZIPInputStream gz = new GZIPInputStream(in)) {
        return gz.readAllBytes();
      }
    }
  }

  private static final class GzipOutputStreamWithLevel extends GZIPOutputStream {
    private GzipOutputStreamWithLevel(final OutputStream out, final int level) throws IOException {
      super(out);
      def.setLevel(level);
    }
  }
}
