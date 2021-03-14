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

import java.io.IOException;

import com.github.luben.zstd.Zstd;

import tech.dnaco.bytes.ByteArraySlice;

public final class ZstdUtil {
  private ZstdUtil() {
    // no-op
  }

  public static ByteArraySlice compress(final byte[] buf, final int off, final int len)
      throws IOException {
    final byte[] zdata = new byte[Math.toIntExact(Zstd.compressBound(len))];
    final long zlen = Zstd.compressByteArray(zdata, 0, zdata.length, buf, off, len, 11);
    if (Zstd.isError(zlen)) throw new IOException("unable to compress data: " + Zstd.getErrorName(zlen));
    return new ByteArraySlice(zdata, 0, Math.toIntExact(zlen));
  }

  public static int decompress(final byte[] dst, final byte[] buf, final int off, final int len)
      throws IOException {
    final long r = Zstd.decompressByteArray(dst, 0, dst.length, buf, off, len);
    if (Zstd.isError(r)) throw new IOException("unable to decompress data: " + Zstd.getErrorName(r));
    return Math.toIntExact(r);
  }
}
