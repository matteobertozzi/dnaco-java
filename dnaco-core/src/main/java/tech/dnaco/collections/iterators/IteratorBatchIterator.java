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

class IteratorBatchIterator<T> implements BatchIterator<T> {
  private final LimitedIterator<T> iterator;

  IteratorBatchIterator(final int batchSize, final Iterator<T> iterator) {
    this.iterator = new LimitedIterator<T>(batchSize, iterator);
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
    iterator.reset();
    return iterator;
  }

  private static class LimitedIterator<T> implements Iterable<T>, Iterator<T> {
    private final Iterator<T> iterator;
    private final int limit;

    private int count;

    public LimitedIterator(final int limit, final Iterator<T> iterator) {
      this.limit = limit;
      this.iterator = iterator;
    }

    public int getLimit() {
      return limit;
    }

    public boolean hasMore() {
      return iterator.hasNext();
    }

    public void reset() {
      count = 0;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext() && count < limit;
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new IllegalStateException();
      }
      count++;
      return iterator.next();
    }

    @Override
    public Iterator<T> iterator() {
      return this;
    }
  }
}
