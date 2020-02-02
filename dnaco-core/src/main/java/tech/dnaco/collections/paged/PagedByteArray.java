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

package tech.dnaco.collections.paged;

import java.util.Arrays;

import tech.dnaco.bytes.ByteArrayReader;
import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.BytesSlice;
import tech.dnaco.collections.ArrayUtil.ByteArrayConsumer;
import tech.dnaco.io.BytesInputStream;
import tech.dnaco.util.BitUtil;

public class PagedByteArray {
  private static final int DEFAULT_PAGES_GROWTH = 16;
  private static final int DEFAULT_PAGE_SIZE = 1024;

  private final int pageSize;

  private int pageItems;
  private int pageCount;
  private byte[] lastPage;
  private byte[][] pages;

  public PagedByteArray() {
    this(DEFAULT_PAGE_SIZE);
  }

  public PagedByteArray(final int pageSize) {
    this.pageSize = BitUtil.nextPow2(pageSize);

    this.pageItems = 0;
    this.pageCount = 1;
    this.lastPage = new byte[this.pageSize];
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

  public BytesSlice getByteSlice() {
    if (pages == null) {
      return new ByteArraySlice(lastPage, 0, pageItems);
    }
    return new PagedByteArraySlice(pages, 0, size());
  }

  public BytesInputStream getInputStream() {
    if (pages == null) {
      return new ByteArrayReader(lastPage, 0, pageItems);
    }
    return new PagedByteArrayReader(pages, 0, size());
  }

  // ================================================================================
  //  PUBLIC read related methods
  // ================================================================================
  public int get(final int index) {
    // TODO: do we need boundary checks
    final int pageIndex = index / pageSize;
    final int pageOffset = index & (pageSize - 1);
    if (pages == null && pageIndex == 0) {
      return lastPage[pageOffset] & 0xff;
    }
    return pages[pageIndex][pageOffset] & 0xff;
  }

  public int forEach(final ByteArrayConsumer consumer) {
    if (pages == null) {
      consumer.accept(lastPage, 0, pageItems);
      return pageItems;
    }

    final int length = size();
    PagedByteArraySlice.forEach(pages, length, consumer);
    return length;
  }

  public int forEach(final int off, int len, final ByteArrayConsumer consumer) {
    if (len == 0) return 0;

    final int pageIndex = off / pageSize;
    final int pageOffset = off & (pageSize - 1);
    if (pages == null && pageIndex == 0) {
      final int avail = Math.min(len, pageItems - pageOffset);
      consumer.accept(lastPage, pageOffset, avail);
      return avail;
    }

    int wlen = 0;
    int avail = Math.min(len, pageItems - pageOffset);
    consumer.accept(pages[pageIndex], pageOffset, avail);
    wlen += avail;
    len -= avail;
    for (int i = pageIndex + 1, n = pageCount - 1; len > 0 && i < n; ++i) {
      avail = Math.min(len, pageSize);
      consumer.accept(pages[i], 0, avail);
      wlen += avail;
      len -= avail;
    }
    if (len > 0) {
      avail = Math.min(len, pageItems);
      consumer.accept(lastPage, 0, avail);
      wlen += avail;
    }
    return wlen;
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
  public void add(final int value) {
    if (pageItems == pageSize) rollPage();
    lastPage[pageItems++] = (byte) (value & 0xff);
  }

  public void add(final byte[] buf) {
    add(buf, 0, buf.length);
  }

  public void add(final byte[] buf, int off, int len) {
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

  public void set(final int index, final int value) {
    final int pageIndex = index / pageSize;
    final int pageOffset = index & (pageSize - 1);
    if (pages == null && pageIndex == 0) {
      lastPage[pageOffset] = (byte) (value & 0xff);
    } else {
      pages[pageIndex][pageOffset] = (byte) (value & 0xff);
    }
  }

  public void set(final int index, final byte[] buf, final int off, final int len) {
    // TODO: speedup
    for (int i = 0; i < len; ++i) {
      set(index + i, buf[off + i] & 0xff);
    }
  }

  public void fill(final int value) {
    final byte bValue = (byte) (value & 0xff);
    if (pages == null) {
      Arrays.fill(lastPage, bValue);
    } else {
      for (int i = 0, n = pages.length; i < n; ++i) {
        Arrays.fill(pages[i], bValue);
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
      pages = new byte[DEFAULT_PAGES_GROWTH][];
      pages[0] = lastPage;
    } else if (pageCount == pages.length) {
      pages = Arrays.copyOf(pages, pages.length + DEFAULT_PAGES_GROWTH);
    }
    lastPage = pages[pageCount];
    if (lastPage == null) {
      lastPage = new byte[pageSize];
    }
    pages[pageCount++] = lastPage;
    pageItems = 0;
  }
}
