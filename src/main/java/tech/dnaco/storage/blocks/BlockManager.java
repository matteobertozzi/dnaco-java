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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.io.IOUtil;
import tech.dnaco.logging.Logger;
import tech.dnaco.storage.blocks.BlockEntryMergeIterator.BlockEntryMergeOptions;
import tech.dnaco.storage.memstore.MemStore;
import tech.dnaco.strings.HumansUtil;

public final class BlockManager {
  private final ArrayList<MemStore> memStores = new ArrayList<>();
  private final ArrayList<DataBlocks> blocks = new ArrayList<>();
  private final AtomicLong entrySeqId = new AtomicLong(0);
  private final File blocksDir;

  public BlockManager(final File blocksDir) {
    this.blocksDir = blocksDir;
  }

  public File getBlocksDir() {
    return blocksDir;
  }

  public void loadBlockIndex() throws IOException {
    // reset the index
    this.blocks.clear();
    this.entrySeqId.set(0);

    // scan the blocks dir
    final File[] blockFiles = blocksDir.listFiles();
    if (ArrayUtil.isEmpty(blockFiles)) {
      Logger.debug("no blocks found for {}", blocksDir);
      return;
    }

    // load data blocks
    final ArrayList<DataBlocks> dataBlocks = new ArrayList<>(blockFiles.length);
    for (int i = 0, n = blockFiles.length; i < n; ++i) {
      try (DataBlocksReader reader = new DataBlocksReader(blockFiles[i])) {
        dataBlocks.add(new DataBlocks(blockFiles[i], reader.getBlocks()));
      }
    }

    // order blocks by seqId asc
    final long startTime = System.nanoTime();
    dataBlocks.sort((a, b) -> Long.compare(a.getSeqId(), a.getSeqId()));
    final long elapsedSort = System.nanoTime() - startTime;
    Logger.debug("sort {} data blocks took {}", dataBlocks.size(), HumansUtil.humanTimeNanos(elapsedSort));

    // update the index
    this.blocks.addAll(dataBlocks);
    this.entrySeqId.set(dataBlocks.get(dataBlocks.size() - 1).getSeqId());
  }

  public void addBlocks(final File blockFile, final List<BlockInfo> dataBlocks) {
    blocks.add(new DataBlocks(blockFile, dataBlocks.toArray(new BlockInfo[0])));
  }

  public MemStore addMemStore(final MemStore memStore) {
    memStores.add(memStore);
    return memStore;
  }

  public long newEntrySeqId() {
    return entrySeqId.incrementAndGet();
  }

  public File newBlockFile() {
    blocksDir.mkdirs();
    return new File(blocksDir, String.format("%020d.blk", newEntrySeqId()));
  }

  public void fullScan(final BlockEntryMergeOptions options, final Predicate<BlockEntry> consumer) throws IOException {
    scanFrom(options, null, consumer);
  }

  public long scanFrom(final BlockEntryMergeOptions options, final ByteArraySlice key, final Predicate<BlockEntry> consumer)
      throws IOException {
    final long startTime = System.nanoTime();
    long rowCount = 0;
    try {
      final ByteArraySlice prevKey = new ByteArraySlice();
      int prevVersion = options.getMaxVersions() - 1;
      try (BlockEntryScanner scanner = scan(options, key)) {
        for (; scanner.hasMore(); scanner.next()) {
          final BlockEntry entry = scanner.readEntry();
          if (options.hasMaxVersions() && entry.getKey().equals(prevKey)) {
            if (prevVersion-- > 0) {
              rowCount++;
              if (!consumer.test(entry)) {
                return rowCount;
              }
            }
          } else {
            rowCount++;
            if (!consumer.test(entry)) {
              return rowCount;
            }
            prevKey.set(entry.getKey().buffer());
            prevVersion = options.getMaxVersions() - 1;
          }
        }
      }
      return rowCount;
    } finally {
      final long elapsedTime = System.nanoTime() - startTime;
      Logger.debug("scan of {} rows took: {}", rowCount, HumansUtil.humanTimeNanos(elapsedTime));
    }
  }

  public void compact() throws IOException {
    if (this.blocks.size() < 2) return;

    try (BlockEntryScanner scanner = scan(new BlockEntryMergeOptions().setMaxVersions(1), null)) {
      while (scanner.hasMore()) {
        final File blockFile = newBlockFile();
        try (final DataBlocksWriter writer = new DataBlocksWriter(blockFile, scanner.stats)) {
          for (; scanner.hasMore(); scanner.next()) {
            final BlockEntry entry = scanner.readEntry();
            writer.add(entry);
            System.out.println("SCAN " + entry);
            if (writer.estimateSize() > (100 << 20)) break;
          }
          final List<BlockInfo> blocks = writer.writeBlocksIndex();
          addBlocks(blockFile, blocks);
        }
      }
    }
  }

  public BlockEntryScanner scan(final BlockEntryMergeOptions options) throws IOException {
    return scan(options, null);
  }

  public BlockEntryScanner scan(final BlockEntryMergeOptions options, final ByteArraySlice key) throws IOException {
    final ArrayList<BlockEntryIterator> dataReaders = new ArrayList<>(blocks.size());
    final BlockStats stats = new BlockStats();
    for (final DataBlocks dataBlocks: blocks) {
      final DataBlocksReader reader = dataBlocks.open();
      if (key != null) {
        reader.seekTo(key);
      } else {
        reader.seekToFirst();
      }
      stats.update(reader.getStats());
      dataReaders.add(reader);
    }
    for (final MemStore memStore: memStores) {
      final BlockEntryIterator reader;
      if (key != null) {
        reader = memStore.newIteratorFrom(key);
      } else {
        reader = memStore.newIterator();
      }
      stats.update(memStore.getStats());
      dataReaders.add(reader);
    }
    return new BlockEntryScanner(options, stats, dataReaders);
  }

  public static class BlockEntryScanner implements AutoCloseable {
    private final ArrayList<BlockEntryIterator> dataReaders;
    private final BlockEntryMergeIterator iter;
    private final BlockStats stats;

    private BlockEntryScanner(final BlockEntryMergeOptions options, final BlockStats stats,
        final ArrayList<BlockEntryIterator> dataReaders) throws IOException {
      this.stats = stats;
      this.dataReaders = dataReaders;
      this.iter = new BlockEntryMergeIterator(options, stats, dataReaders);
    }

    @Override
    public void close() {
      for (final BlockEntryIterator reader: dataReaders) {
        IOUtil.closeQuietly(reader);
      }
    }

    public boolean hasMore() {
      return iter.hasMore();
    }

    public BlockEntry readEntry() throws IOException {
      return iter.readEntry();
    }

    public void next() throws IOException {
      iter.next();
    }
  }

  public static final class DataBlocks {
    private final BlockInfo[] blocks;
    private final File blockFile;
    private final long seqId;

    private DataBlocks(final File blockFile, final BlockInfo[] blocks) {
      this.blockFile = blockFile;
      this.blocks = blocks;
      final String name = blockFile.getName();
      this.seqId = Long.parseLong(name, 0, name.length() - 4, 10);
    }

    public long getSeqId() {
      return seqId;
    }

    public File getBlockFile() {
      return blockFile;
    }

    public BlockInfo[] getBlocks() {
      return blocks;
    }

    public DataBlocksReader open() throws IOException {
      return new DataBlocksReader(blockFile, blocks);
    }

    private BlockInfo seekTo(final ByteArraySlice key) {
      return BlockInfo.seekTo(blocks, key);
    }
  }

  public boolean hasKey(final byte[] key) throws IOException {
    return hasKey(new ByteArraySlice(key));
  }

  public boolean hasKey(final ByteArraySlice key) throws IOException {
    // TODO: bloomfilter + index check + single-block-scan has last hope
    final AtomicBoolean found = new AtomicBoolean(false);
    this.scanFrom(new BlockEntryMergeOptions().setRemoveDeleted(true), key, (entry) -> {
      final int prefix = BytesUtil.prefix(key.rawBuffer(), key.offset(), key.length(), entry.getKey().rawBuffer(), entry.getKey().offset(), entry.getKey().length());
      found.set(prefix == key.length());
      return false;
    });
    return found.get();
  }
}
