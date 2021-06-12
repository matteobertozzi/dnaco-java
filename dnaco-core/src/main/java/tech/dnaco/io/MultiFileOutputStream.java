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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiFileOutputStream extends OutputStream {
  private final OutputStreamSupplier streamSupplier;
  private final long maxFileLength;
  private OutputStream stream;
  private long offset;

  public MultiFileOutputStream(final int maxFileLength, final OutputStreamSupplier streamSupplier) {
    this.streamSupplier = streamSupplier;
    this.maxFileLength = maxFileLength;
    this.offset = 0;
  }

  @Override
  public void close() throws IOException {
    closeStream();
  }

  private void closeStream() throws IOException {
    if (stream != null) {
      stream.close();
      stream = null;
    }
  }

  private void nextStream() throws IOException {
    closeStream();

    stream = streamSupplier.get();
    offset = 0;
  }

  private void ensureStreamOpen() throws IOException {
    if (stream == null) {
      nextStream();
    }
  }

  @Override
  public void write(final int b) throws IOException {
    if (offset >= maxFileLength) {
      nextStream();
    }
    ensureStreamOpen();
    stream.write(b);
  }

  public void write(final byte b[], int off, int len) throws IOException {
    Objects.checkFromIndexSize(off, len, b.length);
    if (len == 0) return;

    while (len > 0) {
      ensureStreamOpen();
      final long avail = maxFileLength - offset;
      if (avail >= len) {
        stream.write(b, off, len);
        offset += len;
        return;
      }

      stream.write(b, off, (int) avail);
      off += avail;
      len -= avail;
      closeStream();
    }
  }

  @FunctionalInterface
  public interface OutputStreamSupplier {
    OutputStream get() throws IOException;
  }
}
