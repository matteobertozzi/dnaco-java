package tech.dnaco.collections.paged;

import java.util.Arrays;

import tech.dnaco.collections.ArrayUtil.ArrayConsumer;
import tech.dnaco.util.BitUtil;

public class PagedArrayList<T> {
  private static final int DEFAULT_PAGES_GROWTH = 16;
  private static final int DEFAULT_PAGE_SIZE = 1024;

  private final int pageSize;

  private int pageItems;
  private int pageCount;
  private T[] lastPage;
  private T[][] pages;

  public PagedArrayList() {
    this(DEFAULT_PAGE_SIZE);
  }

  public PagedArrayList(final int pageSize) {
    this.pageSize = BitUtil.nextPow2(pageSize);

    this.pageItems = 0;
    this.pageCount = 1;
    this.lastPage = (T[]) new Object[this.pageSize];
    this.pages = null;
  }

  public int size() {
    // NOTE: pageCount expected to always be > 0
    return ((pageCount - 1) * pageSize) + pageItems;
  }

  public boolean isEmpty() {
    // NOTE: pageCount expected to always be > 0
    return pageItems == 0 && pageCount == 1;
  }

  public boolean isNotEmpty() {
    return pageItems > 0 || pageCount > 1;
  }

  // ================================================================================
  //  PUBLIC read related methods
  // ================================================================================
  public T get(final int index) {
    // TODO: do we need boundary checks
    final int pageIndex = index / pageSize;
    final int pageOffset = index & (pageSize - 1);
    if (pages == null && pageIndex == 0) {
      return lastPage[pageOffset];
    }
    return pages[pageIndex][pageOffset];
  }

  public void forEach(final ArrayConsumer<T> consumer) {
    for (int i = 0, n = pageCount - 1; i < n; ++i) {
      consumer.accept(pages[i], 0, pageSize);
    }
    consumer.accept(lastPage, 0, pageItems);
  }

  public void forEach(final int off, int len, final ArrayConsumer<T> consumer) {
    if (len == 0) return;

    final int pageIndex = off / pageSize;
    final int pageOffset = off & (pageSize - 1);
    if (pages == null && pageIndex == 0) {
      consumer.accept(lastPage, pageOffset, Math.min(len, pageItems - pageOffset));
    } else {
      int avail = Math.min(len, pageItems - pageOffset);
      consumer.accept(pages[pageIndex], pageOffset, avail);
      len -= avail;
      for (int i = pageIndex + 1, n = pageCount - 1; len > 0 && i < n; ++i) {
        avail = Math.min(len, pageSize);
        consumer.accept(pages[i], 0, avail);
        len -= avail;
      }
      if (len > 0) {
        consumer.accept(lastPage, 0, Math.min(len, pageItems));
      }
    }
  }

  // ================================================================================
  //  PUBLIC clear related methods
  // ================================================================================
  public void clear() {
    clear(false);
  }

  public void clear(final boolean forceEviction) {
    this.lastPage = pages != null ? pages[0] : lastPage;
    this.pageCount = 1;
    this.pageItems = 0;
    if (forceEviction && pages != null) {
      for (int i = 1, n = pages.length; i < n; ++i) {
        this.pages[i] = null;
      }
      this.pages = null;
    }
  }

  // ================================================================================
  //  PUBLIC write related methods
  // ================================================================================
  public void add(final T value) {
    if (pageItems == pageSize) rollPage();
    lastPage[pageItems++] = value;
  }

  public void add(final T[] buf, int off, int len) {
    while (len > 0) {
      int avail = lastPage.length - pageItems;
      if (avail == 0) {
        rollPage();
        avail = pageSize;
      }

      final int copyLen = Math.min(avail, len);
      System.arraycopy(buf, off, lastPage, pageItems, copyLen);
      pageItems += copyLen;
      off += copyLen;
      len -= copyLen;
    }
  }

  public void set(final int index, final T value) {
    final int pageIndex = index / pageSize;
    final int pageOffset = index & (pageSize - 1);
    if (pages == null && pageIndex == 0) {
      lastPage[pageOffset] = value;
    } else {
      pages[pageIndex][pageOffset] = value;
    }
  }

  public void fill(final T value) {
    if (pages == null) {
      Arrays.fill(lastPage, value);
    } else {
      for (int i = 0, n = pages.length; i < n; ++i) {
        Arrays.fill(pages[i], value);
      }
    }
  }

  // ================================================================================
  //  TODO
  //   - Sort + Sorted Search
  //   - Index Of
  // ================================================================================

  // ================================================================================
  //  PRIVATE helpers
  // ================================================================================
  private void rollPage() {
    if (pages == null) {
      pages = (T[][]) new Object[DEFAULT_PAGES_GROWTH][];
      pages[0] = lastPage;
    } else if (pageCount == pages.length) {
      pages = Arrays.copyOf(pages, pages.length + DEFAULT_PAGES_GROWTH);
    }
    lastPage = pages[pageCount];
    if (lastPage == null) {
      lastPage = (T[]) new Object[pageSize];
    }
    pages[pageCount++] = lastPage;
    pageItems = 0;
  }
}