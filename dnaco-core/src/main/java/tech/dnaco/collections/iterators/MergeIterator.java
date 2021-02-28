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

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

public class MergeIterator<T> implements PeekIterator<T> {
  private final PriorityQueue<PeekIterator<T>> queue;

  public MergeIterator(final Iterable<? extends Iterator<? extends T>> iterators, final Comparator<? super T> comparator) {
    this.queue = new PriorityQueue<>(2, (a, b) -> comparator.compare(a.peek(), b.peek()));
    for (final Iterator<? extends T> iterator: iterators) {
      if (iterator.hasNext()) {
        this.queue.add(SimplePeekIterator.newIterator(iterator));
      }
    }
  }

  @Override
  public boolean hasNext() {
    return !queue.isEmpty();
  }

  @Override
  public T peek() {
    final PeekIterator<T> iter = queue.peek();
    return iter != null ? iter.peek() : null;
  }

  @Override
  public T next() {
    final PeekIterator<T> nextIter = queue.remove();
    final T next = nextIter.next();
    if (nextIter.hasNext()) queue.add(nextIter);
    return next;
  }
}
