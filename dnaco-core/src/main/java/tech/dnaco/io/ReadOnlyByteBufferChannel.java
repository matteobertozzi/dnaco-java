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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

public class ReadOnlyByteBufferChannel implements SeekableByteChannel {
  private volatile boolean open;
  private final ByteBuffer data;
  private final int length;

  public ReadOnlyByteBufferChannel(final byte[] data) {
    this(data, 0, data.length);
  }

  public ReadOnlyByteBufferChannel(final byte[] data, final int off, final int len) {
    this(ByteBuffer.wrap(data, off, len));
  }

  public ReadOnlyByteBufferChannel(final ByteBuffer data) {
    this.data = data;
    this.length = data.remaining();
    this.open = true;
  }

  @Override
  public boolean isOpen() {
    return this.open;
  }

  @Override
  public void close() throws IOException {
    this.open = false;
  }

  @Override
  public int read(final ByteBuffer dst) throws IOException {
    final int avail = Math.min(dst.remaining(), data.remaining());
    final ByteBuffer tmp = data.duplicate();
    tmp.limit(tmp.position() + avail);
    dst.put(tmp);
    data.position(data.position() + avail);
    return avail;
  }

  @Override
  public int write(final ByteBuffer src) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long position() throws IOException {
    return length - data.remaining();
  }

  @Override
  public SeekableByteChannel position(final long newPosition) throws IOException {
    data.position(Math.toIntExact(newPosition));
    return this;
  }

  @Override
  public long size() throws IOException {
    return length;
  }

  @Override
  public SeekableByteChannel truncate(final long size) throws IOException {
    throw new UnsupportedOperationException();
  }
}
