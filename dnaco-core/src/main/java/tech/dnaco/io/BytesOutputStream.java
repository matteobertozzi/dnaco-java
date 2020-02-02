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
import java.io.OutputStream;

import tech.dnaco.bytes.BytesSlice;

public abstract class BytesOutputStream extends OutputStream {
  @Override
  public void close() {
    // no-op
  }

  public abstract void reset();

  public abstract boolean isEmpty();

  public boolean isNotEmpty() {
    return !isEmpty();
  }

  public abstract int length();

  @Override
  public abstract void write(int b);

  @Override
  public abstract void write(byte[] buf, int off, int len);

  public void write(final BytesSlice slice) {
    slice.forEach((buf, off, len) -> write(buf, off, len));
  }

  public void write(final BytesSlice slice, final int offset, final int length) {
    slice.forEach(offset, length, (buf, off, len) -> write(buf, off, len));
  }

  public abstract int writeTo(BytesOutputStream stream);

  public abstract int writeTo(OutputStream stream) throws IOException;

  public abstract void writeTo(BytesOutputStream stream, int off, int len);

  public abstract void writeTo(OutputStream stream, int off, int len) throws IOException;
}
