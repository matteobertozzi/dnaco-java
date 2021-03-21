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

package tech.dnaco.collections.iterators;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class AbstractIndexIterator<T> implements Iterator<T> {
  private final long tail;
  private final long head;
  private long offset;

  public AbstractIndexIterator(final int count) {
    this(0, count);
  }

  public AbstractIndexIterator(final long head, final long tail) {
    this.head = head;
    this.tail = tail;
    this.offset = head;
  }

  public void reset() {
    this.offset = head;
  }

  public int size() {
    return (int)(tail - head);
  }

  @Override
  public boolean hasNext() {
    return findNextNonEmptyItem();
  }

  @Override
  public T next() {
    for (; offset < tail; ++offset) {
      final int index = (int)(offset % size());
      if (!isEmpty(index)) {
        offset++;
        return nextItemAt(index);
      }
    }
    throw new NoSuchElementException();
  }

  private boolean findNextNonEmptyItem() {
    for (; offset < tail; ++offset) {
      final int index = (int)(offset % size());
      if (!isEmpty(index)) {
        return true;
      }
    }
    return false;
  }

  protected abstract boolean isEmpty(int index);
  protected abstract T nextItemAt(int index);
}
