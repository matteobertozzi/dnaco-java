/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package tech.dnaco.collections.arrays;

import java.util.AbstractSet;
import java.util.Iterator;

public class ArraySet<T> extends AbstractSet<T> {
  private final T[] array;
  private final int offset;
  private final int length;

  public ArraySet(final T[] array) {
    this(array, 0, ArrayUtil.length(array));
  }

  public ArraySet(final T[] array, final int offset, final int length) {
    this.array = array;
    this.offset = offset;
    this.length = length;
  }

  @Override
  public int size() {
    return length;
  }

  public boolean isNotEmpty() {
    return length != 0;
  }

  @Override
  public Iterator<T> iterator() {
    return new ArrayIterator<>(array, offset, length);
  }
}
