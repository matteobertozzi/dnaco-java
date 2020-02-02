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

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.BytesUtil;

public class DeltaBytesEncoder {
  private final byte[] lastValue;
  private int length;

  public DeltaBytesEncoder(final int maxValueLength) {
    this.lastValue = new byte[maxValueLength];
    this.length = 0;
  }

  public void reset() {
    this.length = 0;
  }

  public int add(final ByteArraySlice value) {
    return add(value.rawBuffer(), value.offset(), value.length());
  }

  public int add(final byte[] buf, final int off, final int len) {
    final int prefix = BytesUtil.prefix(lastValue, 0, length, buf, off, len);
    System.arraycopy(buf, off, lastValue, 0, len);
    this.length = len;
    return prefix;
  }

  public ByteArraySlice getValue() {
    return new ByteArraySlice(lastValue, 0, length);
  }
}
