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

import tech.dnaco.bytes.BytesSlice;

public class DeltaByteDecoder {
  private final byte[] lastValue;
  private int length;

  public DeltaByteDecoder(final int maxValueLength) {
    this.lastValue = new byte[maxValueLength];
    this.length = 0;
  }

  public void reset() {
    this.length = 0;
  }

  public int length() {
    return length;
  }

  public byte[] rawBuffer() {
    return lastValue;
  }

  public void add(final BytesSlice value) {
    value.copyTo(lastValue, 0, value.length());
    this.length = value.length();
  }
}
