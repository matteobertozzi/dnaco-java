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

package tech.dnaco.bytes.encoding;

import java.io.IOException;
import java.io.OutputStream;

import tech.dnaco.bytes.ByteArrayAppender;

public abstract class IntEncoder {
  public static final IntEncoder LITTLE_ENDIAN = new LittleEndian();
  public static final IntEncoder BIG_ENDIAN = new BigEndian();

  // ================================================================================
  //  Fixed Size Array methods
  // ================================================================================
  public void writeFixed8(final byte[] buf, final int off, final long v) { writeFixed(buf, off, v, 1); }
  public void writeFixed16(final byte[] buf, final int off, final long v) { writeFixed(buf, off, v, 2); }
  public void writeFixed24(final byte[] buf, final int off, final long v) { writeFixed(buf, off, v, 3); }
  public void writeFixed32(final byte[] buf, final int off, final long v) { writeFixed(buf, off, v, 4); }
  public void writeFixed40(final byte[] buf, final int off, final long v) { writeFixed(buf, off, v, 5); }
  public void writeFixed48(final byte[] buf, final int off, final long v) { writeFixed(buf, off, v, 6); }
  public void writeFixed56(final byte[] buf, final int off, final long v) { writeFixed(buf, off, v, 7); }
  public void writeFixed64(final byte[] buf, final int off, final long v) { writeFixed(buf, off, v, 8); }

  public abstract void writeFixed(final byte[] buf, int off, final long v, final int bytesWidth);

  // ================================================================================
  //  Fixed Size Stream methods
  // ================================================================================
  public void writeFixed8(final OutputStream stream, final long v) throws IOException  { writeFixed(stream, v, 1); }
  public void writeFixed16(final OutputStream stream, final long v) throws IOException { writeFixed(stream, v, 2); }
  public void writeFixed24(final OutputStream stream, final long v) throws IOException { writeFixed(stream, v, 3); }
  public void writeFixed32(final OutputStream stream, final long v) throws IOException { writeFixed(stream, v, 4); }
  public void writeFixed40(final OutputStream stream, final long v) throws IOException { writeFixed(stream, v, 5); }
  public void writeFixed48(final OutputStream stream, final long v) throws IOException { writeFixed(stream, v, 6); }
  public void writeFixed56(final OutputStream stream, final long v) throws IOException { writeFixed(stream, v, 7); }
  public void writeFixed64(final OutputStream stream, final long v) throws IOException { writeFixed(stream, v, 8); }

  public abstract void writeFixed(final OutputStream stream, final long v, final int bytesWidth) throws IOException;

  public void writeFixed8(final ByteArrayAppender stream, final long v)  { writeFixed(stream, v, 1); }
  public void writeFixed16(final ByteArrayAppender stream, final long v) { writeFixed(stream, v, 2); }
  public void writeFixed24(final ByteArrayAppender stream, final long v) { writeFixed(stream, v, 3); }
  public void writeFixed32(final ByteArrayAppender stream, final long v) { writeFixed(stream, v, 4); }
  public void writeFixed40(final ByteArrayAppender stream, final long v) { writeFixed(stream, v, 5); }
  public void writeFixed48(final ByteArrayAppender stream, final long v) { writeFixed(stream, v, 6); }
  public void writeFixed56(final ByteArrayAppender stream, final long v) { writeFixed(stream, v, 7); }
  public void writeFixed64(final ByteArrayAppender stream, final long v) { writeFixed(stream, v, 8); }

  public abstract void writeFixed(final ByteArrayAppender stream, final long v, final int bytesWidth);

  // ================================================================================
  //  Variable Size methods
  // ================================================================================
  public static int writeUnsignedVarLong(final byte[] buf, final int off, long v) {
    int length = 0;
    while ((v & 0xFFFFFFFFFFFFFF80L) != 0L) {
      buf[off + length++] = (byte)((v & 0x7F) | 0x80);
      v >>>= 7;
    }
    buf[off + length++] = (byte)(v & 0x7F);
    return length;
  }

  public static int writeUnsignedVarLong(final OutputStream stream, long v) throws IOException {
    int length = 0;
    while ((v & 0xFFFFFFFFFFFFFF80L) != 0L) {
      stream.write((int)((v & 0x7F) | 0x80));
      v >>>= 7;
      length++;
    }
    stream.write((int)(v & 0x7F));
    return length + 1;
  }

  public static int writeUnsignedVarLong(final ByteArrayAppender stream, long v) {
    int length = 0;
    while ((v & 0xFFFFFFFFFFFFFF80L) != 0L) {
      stream.add((int)((v & 0x7F) | 0x80));
      v >>>= 7;
      length++;
    }
    stream.add((int)(v & 0x7F));
    return length + 1;
  }

  // ================================================================================
  //  Big Endian methods
  // ================================================================================
  private static final class BigEndian extends IntEncoder {
    private BigEndian() {
      // no-op
    }

    @Override
    @SuppressWarnings("fallthrough")
    public void writeFixed(final byte[] buf, int off, final long v, final int bytesWidth) {
      switch (bytesWidth) {
        case 8: buf[off++] = (byte)((v >>> 56) & 0xff);
        case 7: buf[off++] = (byte)((v >>> 48) & 0xff);
        case 6: buf[off++] = (byte)((v >>> 40) & 0xff);
        case 5: buf[off++] = (byte)((v >>> 32) & 0xff);
        case 4: buf[off++] = (byte)((v >>> 24) & 0xff);
        case 3: buf[off++] = (byte)((v >>> 16) & 0xff);
        case 2: buf[off++] = (byte)((v >>>  8) & 0xff);
        case 1: buf[off] = (byte)((v) & 0xff);
      }
    }

    @Override
    public void writeFixed(final OutputStream stream, final long v, final int bytesWidth) throws IOException {
      final byte[] buf = new byte[bytesWidth];
      writeFixed(buf, 0, v, bytesWidth);
      stream.write(buf, 0, bytesWidth);
    }

    @Override
    public void writeFixed(final ByteArrayAppender stream, final long v, final int bytesWidth) {
      final byte[] buf = new byte[bytesWidth];
      writeFixed(buf, 0, v, bytesWidth);
      stream.add(buf, 0, bytesWidth);
    }
  }

  // ================================================================================
  //  Little Endian methods
  // ================================================================================
  private static final class LittleEndian extends IntEncoder {
    private LittleEndian() {
      // no-op
    }

    @Override
    @SuppressWarnings("fallthrough")
    public void writeFixed(final byte[] buf, final int off, final long v, final int bytesWidth) {
      switch (bytesWidth) {
        case 8: buf[off + 7] = ((byte)((v >>> 56) & 0xff));
        case 7: buf[off + 6] = ((byte)((v >>> 48) & 0xff));
        case 6: buf[off + 5] = ((byte)((v >>> 40) & 0xff));
        case 5: buf[off + 4] = ((byte)((v >>> 32) & 0xff));
        case 4: buf[off + 3] = ((byte)((v >>> 24) & 0xff));
        case 3: buf[off + 2] = ((byte)((v >>> 16) & 0xff));
        case 2: buf[off + 1] = ((byte)((v >>> 8) & 0xff));
        case 1: buf[off] = (byte)(v & 0xff);
      }
    }

    @Override
    public void writeFixed(final OutputStream stream, final long v, final int bytesWidth) throws IOException {
      for (int i = 0; i < bytesWidth; ++i) {
        stream.write((byte)((v >>> (i << 3)) & 0xff));
      }
    }

    @Override
    public void writeFixed(final ByteArrayAppender stream, final long v, final int bytesWidth) {
      for (int i = 0; i < bytesWidth; ++i) {
        stream.add((byte)((v >>> (i << 3)) & 0xff));
      }
    }
  }
}
