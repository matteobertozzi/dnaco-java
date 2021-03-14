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
