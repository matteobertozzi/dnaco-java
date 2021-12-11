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
import java.io.InputStream;

import tech.dnaco.io.IOUtil;

public abstract class IntDecoder {
  public static final IntDecoder LITTLE_ENDIAN = new LittleEndian();
  public static final IntDecoder BIG_ENDIAN = new BigEndian();

  // ================================================================================
  //  Fixed Size Array methods
  // ================================================================================
  public int readFixed8(final byte[] buf, final int off)  { return (int) readFixed(buf, off, 1); }
  public int readFixed16(final byte[] buf, final int off) { return (int) readFixed(buf, off, 2); }
  public int readFixed24(final byte[] buf, final int off) { return (int) readFixed(buf, off, 3); }
  public int readFixed32(final byte[] buf, final int off) { return (int) readFixed(buf, off, 4); }
  public long readFixed40(final byte[] buf, final int off) { return readFixed(buf, off, 5); }
  public long readFixed48(final byte[] buf, final int off) { return readFixed(buf, off, 6); }
  public long readFixed56(final byte[] buf, final int off) { return readFixed(buf, off, 7); }
  public long readFixed64(final byte[] buf, final int off) { return readFixed(buf, off, 8); }

  public abstract long readFixed(final byte[] buf, int off, final int bytesWidth);

  // ================================================================================
  //  Fixed Size Stream methods
  // ================================================================================
  public int readFixed8(final InputStream stream) throws IOException { return (int) readFixed(stream, 1); }
  public int readFixed16(final InputStream stream) throws IOException { return (int) readFixed(stream, 2); }
  public int readFixed24(final InputStream stream) throws IOException { return (int) readFixed(stream, 3); }
  public int readFixed32(final InputStream stream) throws IOException { return (int) readFixed(stream, 4); }
  public long readFixed40(final InputStream stream) throws IOException { return readFixed(stream, 5); }
  public long readFixed48(final InputStream stream) throws IOException { return readFixed(stream, 6); }
  public long readFixed56(final InputStream stream) throws IOException { return readFixed(stream, 7); }
  public long readFixed64(final InputStream stream) throws IOException { return readFixed(stream, 8); }

  public abstract long readFixed(final InputStream stream, final int bytesWidth) throws IOException;

  // ================================================================================
  //  Variable Size methods
  // ================================================================================
  public static long readUnsignedVarLong(final byte[] buf, final int off) {
    long value = 0;
    int shift = 0;
    long b;
    for (int i = 0; ((b = buf[off + i]) & 0x80) != 0; ++i) {
      value |= (b & 0x7F) << shift;
      shift += 7;
    }
    return value | (b << shift);
  }

  public static int readUnsignedVarInt(final byte[] buf, final int off) {
    return Math.toIntExact(readUnsignedVarLong(buf, off));
  }

  public static long readUnsignedVarLong(final InputStream stream) throws IOException {
    long value = 0;
    int shift = 0;
    long b;
    while (((b = stream.read()) & 0x80) != 0) {
      value |= (b & 0x7F) << shift;
      shift += 7;
    }
    return value | (b << shift);
  }

  public static int readUnsignedVarInt(final InputStream stream) throws IOException {
    return Math.toIntExact(readUnsignedVarLong(stream));
  }

  // ================================================================================
  //  Big Endian methods
  // ================================================================================
  private static final class BigEndian extends IntDecoder {
    private BigEndian() {
      // no-op
    }

    @Override
    @SuppressWarnings("fallthrough")
    public long readFixed(final byte[] buf, int off, final int bytesWidth) {
      long result = 0;
      switch (bytesWidth) {
        case 8: result  = (((long)buf[off++] & 0xff) << 56);
        case 7: result += (((long)buf[off++] & 0xff) << 48);
        case 6: result += (((long)buf[off++] & 0xff) << 40);
        case 5: result += (((long)buf[off++] & 0xff) << 32);
        case 4: result += (((long)buf[off++] & 0xff) << 24);
        case 3: result += (((long)buf[off++] & 0xff) << 16);
        case 2: result += (((long)buf[off++] & 0xff) <<  8);
        case 1: result += (((long)buf[off] & 0xff));
      }
      return result;
    }

    @Override
    public long readFixed(final InputStream stream, final int bytesWidth) throws IOException {
      final byte[] buf = IOUtil.readNBytes(stream, bytesWidth);
      return readFixed(buf, 0, bytesWidth);
    }
  }

  // ================================================================================
  //  Little Endian methods
  // ================================================================================
  private static final class LittleEndian extends IntDecoder {
    private LittleEndian() {
      // no-op
    }

    @Override
    public long readFixed(final byte[] buf, final int off, final int bytesWidth) {
      long result = 0;
      for (int i = 0; i < bytesWidth; ++i) {
        result += (((long)buf[off + i] & 0xff) << (i << 3));
      }
      return result;
    }

    @Override
    public long readFixed(final InputStream stream, final int bytesWidth) throws IOException {
      long result = 0;
      for (long i = 0; i < bytesWidth; ++i) {
        final long v = stream.read() & 0xff;
        result += (v << (i << 3));
      }
      return result;
    }
  }
}
