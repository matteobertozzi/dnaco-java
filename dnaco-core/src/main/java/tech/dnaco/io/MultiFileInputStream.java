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

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public class MultiFileInputStream extends InputStream {
  private final InputStreamSupplier streamSupplier;
  private InputStream stream;

  public MultiFileInputStream(final InputStreamSupplier streamSupplier) throws IOException {
    this.streamSupplier = streamSupplier;
    this.stream = streamSupplier.get();
  }

  @Override
  public void close() throws IOException {
    if (stream != null) {
      stream.close();
    }
  }

  private void nextStream() throws IOException {
    if (stream != null) {
      stream.close();
      stream = null;
    }
    stream = streamSupplier.get();
  }

  public int available() throws IOException {
    if (stream == null) {
      return 0; // no way to signal EOF from available()
    }
    return stream.available();
  }

  public int read() throws IOException {
    while (stream != null) {
      final int c = stream.read();
      if (c != -1) {
          return c;
      }
      nextStream();
    }
    return -1;
  }

  public int read(final byte b[], final int off, final int len) throws IOException {
    Objects.checkFromIndexSize(off, len, b.length);
    if (len == 0) return 0;

    do {
      final int n = stream.read(b, off, len);
      if (n > 0) {
        return n;
      }
      nextStream();
    } while (stream != null);
    return -1;
  }

  @FunctionalInterface
  public interface InputStreamSupplier {
    InputStream get() throws IOException;
  }
}
