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

package tech.dnaco.bytes;

import java.io.IOException;
import java.io.OutputStream;

import tech.dnaco.io.BytesInputStream;

public class ByteArrayReader extends BytesInputStream {
  private final byte[] buffer;
  private final int bufOff;
  private final int bufLen;

  private int rpos;

  public ByteArrayReader(final byte[] buf) {
    this(buf, 0, buf.length);
  }

  public ByteArrayReader(final byte[] buf, final int off, final int len) {
    this.buffer = buf;
    this.bufOff = off;
    this.bufLen = len;
    this.rpos = bufOff;
  }

  public ByteArrayReader(final ByteArraySlice slice) {
    this(slice.rawBuffer(), slice.offset(), slice.length());
  }

  @Override
  public int available() {
    return (bufOff + bufLen) - rpos;
  }

  @Override
  public void reset() {
    this.rpos = bufOff;
  }

  @Override
  public void seekTo(final int offset) {
    this.rpos = bufOff + offset;
  }

  @Override
  public boolean isEmpty() {
    return bufLen == 0;
  }

  @Override
  public int length() {
    return bufLen;
  }

  public byte[] rawBuffer() {
    return buffer;
  }

  public int offset() {
    return bufOff;
  }

  public int readOffset() {
    return rpos;
  }

  @Override
  public long skip(final long n) {
    this.rpos += n;
    return n;
  }

  @Override
  public int read() {
    return available() > 0 ? (buffer[rpos++] & 0xff) : -1;
  }

  @Override
  public byte[] readNBytes(final int len) {
    // TODO: check if available
    final byte[] block = new byte[len];
    System.arraycopy(buffer, rpos, block, 0, len);
    rpos += len;
    return block;
  }

  public ByteArraySlice readSlice(final int len) {
    final int off = rpos;
    rpos += len;
    return new ByteArraySlice(buffer, off, len);
  }

  @Override
  public void copyTo(final int blockLen, final OutputStream stream) throws IOException {
    stream.write(buffer, rpos, blockLen);
    rpos += blockLen;
  }
}
