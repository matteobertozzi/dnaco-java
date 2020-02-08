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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.collections.ArrayUtil;
import tech.dnaco.io.IOUtil;
import tech.dnaco.logging.Logger;
import tech.dnaco.storage.block.BlockEntryMergeIterator.BlockEntryMergeOptions;
import tech.dnaco.strings.HumansUtil;

public final class BlockManager {
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
    long startTime = System.nanoTime();
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

  public long newEntrySeqId() {
    return entrySeqId.incrementAndGet();
  }

  public File newBlockFile() {
    blocksDir.mkdirs();
    return new File(blocksDir, String.format("%020d.blk", newEntrySeqId()));
  }

  public void fullScan(final BlockEntryMergeOptions options, final Consumer<BlockEntry> consumer) throws IOException {
    scanFrom(options, null, consumer);
  }

  public void scanFrom(final BlockEntryMergeOptions options, final ByteArraySlice key, final Consumer<BlockEntry> consumer)
      throws IOException {
    try (BlockEntryScanner scanner = scan(options, key)) {
      for (; scanner.hasMore(); scanner.next()) {
        final BlockEntry entry = scanner.readEntry();
        consumer.accept(entry);
      }
    }
  }

  public void compact() throws IOException {
    try (BlockEntryScanner scanner = scan(new BlockEntryMergeOptions(), null)) {
      long seqId = 0;
      while (scanner.hasMore()) {
        File diskFile = new File(String.format("disk-%08d", seqId++));
        try (final DataBlocksWriter writer = new DataBlocksWriter(diskFile, scanner.stats)) {
          for (; scanner.hasMore(); scanner.next()) {
            final BlockEntry entry = scanner.readEntry();
            writer.add(entry);
            if (writer.estimateSize() > (100 << 20)) break;
          }
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
    for (DataBlocks dataBlocks: blocks) {
      final DataBlocksReader reader = dataBlocks.open();
      if (key != null) {
        reader.seekTo(key);
      } else {
        reader.seekToFirst();
      }
      stats.update(reader.getStats());
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
      for (BlockEntryIterator reader: dataReaders) {
        IOUtil.closeQuietly((AutoCloseable)reader);
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

    public BlockInfo seekTo(final ByteArraySlice key) {
      return BlockInfo.seekTo(blocks, key);
    }
  }
}
