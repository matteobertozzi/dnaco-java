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

package tech.dnaco.storage.block;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class BlockEntryMergeIterator {
  private final PriorityQueue<NextIterator> queue;

  public BlockEntryMergeIterator(final List<BlockEntryIterator> iterators) throws IOException {
    queue = new PriorityQueue<>(iterators.size(), BLOCK_ENTRY_ITERATOR_COMPARATOR);
    for (final BlockEntryIterator iterator : iterators) {
      final NextIterator nextIterator = new NextIterator(iterator);
      if (nextIterator.fetchNext()) {
        //System.out.println("-> " + nextIterator.nextEntry);
        queue.add(nextIterator);
      }
    }
  }

  public boolean hasMore() {
    return !queue.isEmpty();
  }

  public BlockEntry readEntry() throws IOException {
    return queue.peek().getBlockEntry();
  }

  public void next() throws IOException {
    final NextIterator nextIter = queue.remove();
    if (nextIter.fetchNext()) queue.add(nextIter);
  }

  private static final Comparator<NextIterator> BLOCK_ENTRY_ITERATOR_COMPARATOR = new Comparator<>() {
    @Override
    public int compare(final NextIterator a, final NextIterator b) {
      return BlockEntry.compare(a.getBlockEntry(), b.getBlockEntry());
    }
  };

  private static final class NextIterator {
    private final BlockEntryIterator iterator;
    private BlockEntry nextEntry;

    private NextIterator(final BlockEntryIterator iterator) {
      this.iterator = iterator;
    }

    private BlockEntry getBlockEntry() {
      return nextEntry;
    }

    private boolean fetchNext() throws IOException {
      if (iterator.hasMoreEntries()) {
        this.nextEntry = iterator.nextEntry();
        return true;
      }
      return false;
    }

    @Override
    public String toString() {
      return "NextIterator [iterator=" + iterator + "]";
    }
  }

  public static void main(final String[] args) throws Exception {
    final File[] walFiles = new File("DNAdb").listFiles();
    final ArrayList<BlockEntryIterator> dataReaders = new ArrayList<>();
    for (int i = 0; i < walFiles.length; ++i) {
      dataReaders.add(new DataBlocksReader(walFiles[i]));
    }

    // 74, 85, 119, 158
    int rowCount = 0;
    final BlockEntryMergeIterator iter = new BlockEntryMergeIterator(dataReaders);
    while (iter.hasMore()) {
      final BlockEntry entry = iter.readEntry();
      System.out.println(" -> MAIN -> " + entry);
      iter.next();
      rowCount++;
      if (rowCount >= 10) break;
    }
    System.out.println("-> rowCount=" + rowCount);

    for (final BlockEntryIterator reader: dataReaders) {
      ((AutoCloseable)reader).close();
    }
  }
}
