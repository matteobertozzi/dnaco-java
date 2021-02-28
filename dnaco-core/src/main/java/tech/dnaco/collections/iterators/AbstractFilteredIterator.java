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

public abstract class AbstractFilteredIterator<TIn, TOut> implements PeekIterator<TOut> {
  protected final Iterator<? extends TIn> iterator;

  private boolean initialized;
  private boolean hasItem;
  private TOut nextItem;

  protected AbstractFilteredIterator(final Iterator<? extends TIn> iterator) {
    this.iterator = iterator;
    this.initialized = false;
  }

  @Override
  public boolean hasNext() {
    initializeIfNot();
    return hasItem;
  }

  @Override
  public TOut peek() {
    initializeIfNot();
    return nextItem;
  }

  @Override
  public TOut next() {
    initializeIfNot();

    if (!hasItem) {
      throw new NoSuchElementException();
    }

    final TOut value = nextItem;
    computeNext();
    return value;
  }

  @Override
  public void remove() {
    iterator.remove();
  }

  private void initializeIfNot() {
    if (initialized) return;

    computeNext();
    initialized = true;
  }

  protected void setNoMoreItems() {
    this.nextItem = null;
    this.hasItem = false;
  }

  protected void setNextItem(final TOut value) {
    this.nextItem = value;
    this.hasItem = true;
  }

  @SuppressWarnings("unchecked")
  protected TIn peekNext() {
    if (iterator instanceof PeekIterator) {
      return ((PeekIterator<TIn>)iterator).peek();
    }
    throw new UnsupportedOperationException();
  }

  protected abstract void computeNext();
}
