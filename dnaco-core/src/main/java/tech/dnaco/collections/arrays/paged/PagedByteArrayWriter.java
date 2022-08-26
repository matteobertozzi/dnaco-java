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

package tech.dnaco.collections.arrays.paged;

import java.io.IOException;
import java.io.OutputStream;

import tech.dnaco.bytes.ByteArrayAppender;
import tech.dnaco.io.BytesOutputStream;

public class PagedByteArrayWriter extends BytesOutputStream {
  private final PagedByteArray buf;

  public PagedByteArrayWriter(final int blockSize) {
    this(new PagedByteArray(blockSize));
  }

  public PagedByteArrayWriter(final PagedByteArray buffer) {
    this.buf = buffer;
  }

  public byte[] toByteArray() {
    return buf.toByteArray();
  }

  @Override
  public void reset() {
    buf.clear();
  }

  @Override
  public boolean isEmpty() {
    return buf.isEmpty();
  }

  @Override
  public int length() {
    return buf.size();
  }

  @Override
  public void write(final int b) {
    buf.add(b);
  }

  @Override
  public void write(final byte[] buf, final int off, final int len) {
    this.buf.add(buf, off, len);
  }

  public int writeTo(final ByteArrayAppender stream) {
    try {
      return this.buf.forEach(stream::add);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int writeTo(final BytesOutputStream stream) {
    try {
      return this.buf.forEach(stream::write);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int writeTo(final OutputStream stream) throws IOException {
    return this.buf.forEach(stream::write);
  }

  @Override
  public void writeTo(final BytesOutputStream stream, final int off, final int len) {
    try {
      this.buf.forEach(off, len, stream::write);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeTo(final OutputStream stream, final int off, final int len) throws IOException {
    this.buf.forEach(off, len, stream::write);
  }
}
