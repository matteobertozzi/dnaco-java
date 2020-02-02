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

import tech.dnaco.collections.ArrayUtil.ByteArrayConsumer;

public interface BytesSlice extends Comparable<BytesSlice> {
  int length();
  boolean isEmpty();
  boolean isNotEmpty();

  int get(int index);

  void copyTo(byte[] buf, int off, int len);

  void forEach(ByteArrayConsumer consumer);
  void forEach(int off, int len, ByteArrayConsumer consumer);

  public static final BytesSlice EMPTY_SLICE = new BytesSlice() {
    @Override public int compareTo(BytesSlice o) { return o.isEmpty() ? 0 : 1; }
    @Override public int length() { return 0; }
    @Override public boolean isEmpty() { return true; }
    @Override public boolean isNotEmpty() { return false; }
    @Override public int get(int index) { return -1; }
    @Override public void copyTo(byte[] buf, int off, int len) { }
    @Override public void forEach(ByteArrayConsumer consumer) { }
    @Override public void forEach(int off, int len, ByteArrayConsumer consumer) { }
  };
}
