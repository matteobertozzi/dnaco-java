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

import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream {
  private final ByteBuffer buf;
  private final long length;

  public ByteBufferInputStream(final ByteBuffer buf, final long length) {
    this.buf = buf;
    this.length = length;
  }

  public long length() {
    return length;
  }

  public void seekTo(final long offset) {
    buf.position(Math.toIntExact(offset));
  }

  @Override
  public int available() {
    return buf.remaining();
  }

  @Override
  public int read() {
    return buf.hasRemaining() ? buf.get() & 0xFF : -1;
  }

  @Override
  public int read(final byte[] bytes, final int off, final int len) {
    final int avail = buf.remaining();
    if (avail <= 0) return -1;

    final int rd = Math.min(len, avail);
    buf.get(bytes, off, rd);
    return rd;
  }

  @Override
  public byte[] readNBytes(final int len) {
    final byte[] bytes = new byte[len];
    buf.get(bytes, 0, len);
    return bytes;
  }

  /* Java 13
  public ByteBuffer slice(final int length) {
    final int offset = buf.position();
    buf.position(offset + length);
    return buf.slice(offset, length);
  }
  */
}
