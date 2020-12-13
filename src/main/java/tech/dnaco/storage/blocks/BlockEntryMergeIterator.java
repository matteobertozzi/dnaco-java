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

package tech.dnaco.storage.blocks;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import tech.dnaco.logging.Logger;
import tech.dnaco.strings.HumansUtil;

public class BlockEntryMergeIterator {
  private final PriorityQueue<NextIterator> queue;
  private final BlockEntryMergeOptions options;

  public BlockEntryMergeIterator(final BlockEntryMergeOptions options, final BlockStats stats,
      final List<BlockEntryIterator> iterators) throws IOException {
    this.options = options;
    this.queue = new PriorityQueue<>(iterators.size(), BLOCK_ENTRY_ITERATOR_COMPARATOR);

    final long startTime = System.nanoTime();
    for (final BlockEntryIterator iterator : iterators) {
      final NextIterator nextIterator = new NextIterator(iterator);
      if (nextIterator.fetchNext(options)) {
        queue.add(nextIterator);
      }
    }
    final long elapsed = System.nanoTime() - startTime;
    Logger.debug("iterator prepare time: {}", HumansUtil.humanTimeNanos(elapsed));
  }

  public boolean hasMore() {
    return !queue.isEmpty();
  }

  public BlockEntry readEntry() throws IOException {
    return queue.peek().getBlockEntry();
  }

  public void next() throws IOException {
    final NextIterator nextIter = queue.remove();
    if (nextIter.fetchNext(options)) queue.add(nextIter);
  }

  private static final Comparator<NextIterator> BLOCK_ENTRY_ITERATOR_COMPARATOR = new Comparator<>() {
    @Override
    public int compare(final NextIterator a, final NextIterator b) {
      return BlockEntry.compare(a.getBlockEntry(), b.getBlockEntry());
    }
  };

  public static final class BlockEntryMergeOptions {
    private boolean removeDeleted = false;
    private long minSeqId = Long.MIN_VALUE;
    private long minTimestamp = Long.MIN_VALUE;
    private int versions = Integer.MAX_VALUE;

    public BlockEntryMergeOptions setRemoveDeleted(final boolean removeDeleted) {
      this.removeDeleted = removeDeleted;
      return this;
    }

    public BlockEntryMergeOptions setMinSeqId(final long seqId) {
      this.minSeqId = seqId;
      return this;
    }

    public BlockEntryMergeOptions setMinTimestamp(final long timestamp) {
      this.minTimestamp = timestamp;
      return this;
    }

    public BlockEntryMergeOptions setMaxVersions(final int versions) {
      this.versions = versions;
      return this;
    }

    public boolean hasMaxVersions() {
      return versions != Integer.MAX_VALUE;
    }

    public int getMaxVersions() {
      return versions;
    }

    public boolean isVisible(final BlockEntry entry) {
      if (removeDeleted && entry.isDeleted()) return false;
      if (entry.getSeqId() < minSeqId) return false;
      if (entry.getTimestamp() < minTimestamp) return false;
      return true;
    }
  }

  private static final class NextIterator {
    private final BlockEntryIterator iterator;
    private BlockEntry nextEntry;

    private NextIterator(final BlockEntryIterator iterator) {
      this.iterator = iterator;
    }

    private BlockEntry getBlockEntry() {
      return nextEntry;
    }

    private boolean fetchNext(final BlockEntryMergeOptions options) throws IOException {
      while (iterator.hasMoreEntries()) {
        this.nextEntry = iterator.nextEntry();
        if (options.isVisible(this.nextEntry)) {
          return true;
        }
      }
      this.nextEntry = null;
      return false;
    }

    @Override
    public String toString() {
      return "BlockNextIterator [iterator=" + iterator + "]";
    }
  }
}
