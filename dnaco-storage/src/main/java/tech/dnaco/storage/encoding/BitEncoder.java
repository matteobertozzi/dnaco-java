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

import java.io.IOException;
import java.io.OutputStream;

import tech.dnaco.bytes.encoding.IntEncoder;

import tech.dnaco.bytes.encoding.IntEncoder;

public class BitEncoder implements AutoCloseable {
  private final OutputStream stream;
  private final int mask;
  private final int width;

  private long vBuffer = 0;
  private int vCount = 0;
  
  public BitEncoder(final OutputStream stream, final int width) {
    this.stream = stream;
    this.mask = (1 << width) - 1;
    this.width = width;
  }

  @Override
  public void close() throws IOException {
    flush();
  }

  public void add(final int v) throws IOException {
    vBuffer = (vBuffer << width) | (v & mask);
    vCount++;
    if (((vCount * width) & 7) == 0) {
      flush((vCount * width) / 8);
    }
  }

  public void flush() throws IOException {
    if (vCount == 0) return;

    while (((vCount * width) & 7) != 0) {
      vBuffer = (vBuffer << width);
      vCount++;
    }

    flush((vCount * width) / 8);
  }

  private void flush(final int bytesWidth) throws IOException {
    IntEncoder.BIG_ENDIAN.writeFixed(stream, vBuffer, bytesWidth);
    vBuffer = 0;
    vCount = 0;
  }
}
