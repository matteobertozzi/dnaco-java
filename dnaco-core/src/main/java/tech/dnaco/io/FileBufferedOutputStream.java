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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;

public class FileBufferedOutputStream extends OutputStream {
  private final byte[] buffer = new byte[4 << 10];
  private final FileOutputStream stream;

  private long position = 0;
  private int bufLength = 0;

  public FileBufferedOutputStream(final File file) throws FileNotFoundException {
    this(new FileOutputStream(file));
  }

  public FileBufferedOutputStream(final FileOutputStream stream) {
    this.stream = stream;
  }

  @Override
  public void close() throws IOException {
    if (bufLength > 0) {
      flushBuffer();
    }
    stream.close();
  }

  public FileChannel getChannel() {
    return stream.getChannel();
  }

  public long position() throws IOException {
    //System.out.println(" ----> READ POSITION " + stream.getChannel().position() + " BUF-LEN " + bufLength + " POSITION " + position);
    //return stream.getChannel().position() + bufLength;
    return position;
  }

  @Override
  public void write(final int b) throws IOException {
    if (bufLength == buffer.length) {
      flushBuffer();
    }
    buffer[bufLength++] = (byte) (b & 0xff);
    this.position++;
  }

  @Override
  public void write(final byte[] buf, int off, int len) throws IOException {
    this.position += len;

    if (bufLength > 0) {
      final int avail = Math.min(len, buffer.length - bufLength);
      System.arraycopy(buf, off, buffer, bufLength, avail);
      bufLength += avail;
      off += avail;
      len -= avail;

      if (bufLength >= buffer.length) {
        flushBuffer();
      }
    }

    if (len < buffer.length) {
      System.arraycopy(buf, off, buffer, bufLength, len);
      bufLength += len;
    } else if (len > 0) {
      stream.write(buf, off, len);
    }
  }

  private void flushBuffer() throws IOException {
    stream.write(buffer, 0, bufLength);
    bufLength = 0;
  }
}
