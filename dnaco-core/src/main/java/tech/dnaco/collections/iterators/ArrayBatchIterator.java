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

class ArrayBatchIterator<T> implements BatchIterator<T> {
  private final LimitedArrayIterator<T> iterator;

  ArrayBatchIterator(final int batchSize, final T[] items) {
    this.iterator = new LimitedArrayIterator<>(batchSize, items);
  }

  @Override
  public int getBatchSize() {
    return iterator.getLimit();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasMore();
  }

  @Override
  public Iterable<T> next() {
    iterator.nextEndIndex();
    return iterator;
  }

  private static final class LimitedArrayIterator<T> implements Iterable<T>, Iterator<T> {
    private final T[] items;
    private final int limit;

    private int index;
    private int endIndex;

    public LimitedArrayIterator(final int limit, final T[] items) {
      this.items = items;
      this.limit = limit;
      this.index = 0;
      this.endIndex = Math.min(limit, items.length);
    }

    public int getLimit() {
      return limit;
    }

    public void nextEndIndex() {
      if (index >= items.length) {
        throw new NoSuchElementException();
      }
      endIndex = Math.min(index + limit, items.length);
    }

    public boolean hasMore() {
      return index < items.length;
    }

    @Override
    public boolean hasNext() {
      return index < endIndex;
    }

    @Override
    public T next() {
      if (index >= endIndex) {
        throw new NoSuchElementException();
      }
      return items[index++];
    }

    @Override
    public Iterator<T> iterator() {
      return this;
    }
  }
}
