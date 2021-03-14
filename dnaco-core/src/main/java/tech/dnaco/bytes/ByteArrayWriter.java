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

import tech.dnaco.io.BytesOutputStream;

public class ByteArrayWriter extends BytesOutputStream {
  private final byte[] buffer;
  private final int bufOff;
  private final int bufLen;

  private int wpos;

  public ByteArrayWriter(final byte[] buf) {
    this(buf, 0, buf.length);
  }

  public ByteArrayWriter(final byte[] buf, final int off, final int len) {
    this.buffer = buf;
    this.bufOff = off;
    this.bufLen = len;
    this.wpos = off;
  }

  @Override
  public void reset() {
    this.wpos = bufOff;
  }

  @Override
  public boolean isEmpty() {
    return bufLen == 0;
  }

  public int available() {
    return bufLen - (wpos - bufOff);
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

  public int writeOffset() {
    return wpos - bufOff;
  }

  @Override
  public void write(final int b) {
    buffer[wpos++] = (byte) (b & 0xff);
  }

  public void write(final byte[] buf) {
    write(buf, 0, buf.length);
  }

  @Override
  public void write(final byte[] buf, final int off, final int len) {
    System.arraycopy(buf, off, this.buffer, this.wpos, len);
    wpos += len;
  }

  @Override
  public int writeTo(final BytesOutputStream stream) {
    stream.write(buffer, bufOff, bufLen);
    return bufLen;
  }

  @Override
  public int writeTo(final OutputStream stream) throws IOException {
    stream.write(buffer, bufOff, bufLen);
    return bufLen;
  }

  @Override
  public void writeTo(final BytesOutputStream stream, final int off, final int len) {
    stream.write(this.buffer, this.bufOff + off, len);
  }

  @Override
  public void writeTo(final OutputStream stream, final int off, final int len) throws IOException {
    stream.write(this.buffer, this.bufOff + off, len);
  }
}
