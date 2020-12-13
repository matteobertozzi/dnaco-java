package tech.dnaco.storage.memstore;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.logging.Logger;
import tech.dnaco.storage.blocks.BlockEntry;
import tech.dnaco.storage.blocks.BlockEntryIterator;
import tech.dnaco.storage.blocks.BlockInfo;
import tech.dnaco.storage.blocks.BlockManager;
import tech.dnaco.storage.blocks.BlockStats;
import tech.dnaco.storage.blocks.DataBlocksWriter;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.telemetry.Histogram;
import tech.dnaco.telemetry.TelemetryCollector;

public class MemStore {
  private final Histogram memStoreFlushTime = new TelemetryCollector.Builder()
      .setName("storage_memstore_flush_time")
      .setLabel("Storage MemStore Flush Time")
      .setUnit(HumansUtil.HUMAN_TIME_NANOS)
      .register(new Histogram(Histogram.DEFAULT_DURATION_BOUNDS_NS));

  private final ConcurrentSkipListSet<BlockEntry> entries = new ConcurrentSkipListSet<>(BlockEntry::compareKey);
  private final AtomicLong estimateSize = new AtomicLong(0);
  private final BlockStats stats = new BlockStats();

  public BlockStats getStats() {
    return stats;
  }

  public long add(final BlockEntry entry) {
    entries.add(entry);
    stats.update(entry);
    return estimateSize.addAndGet(entry.estimateSize());
  }

  public void addAll(final List<BlockEntry> entries) {
    for (final BlockEntry entry: entries) {
      add(entry);
    }
  }

  public File flush(final BlockManager blockManager) throws IOException {
    final long startTime = System.nanoTime();
    final File blockFile = blockManager.newBlockFile();
    try (DataBlocksWriter writer = new DataBlocksWriter(blockFile, stats)) {
      for (final BlockEntry entry: entries) {
        writer.add(entry);
      }
      final List<BlockInfo> blocks = writer.writeBlocksIndex();
      blockManager.addBlocks(blockFile, blocks);
    }
    final long elapsed = System.nanoTime() - startTime;
    memStoreFlushTime.add(elapsed);
    Logger.info("memstore flush took {} ", HumansUtil.humanTimeNanos(elapsed));
    return blockFile;
  }

  public BlockEntryIterator newIterator() {
    return new MemStoreReader(entries);
  }

  public BlockEntryIterator newIteratorFrom(final ByteArraySlice key) {
    return new MemStoreReader(entries, key);
  }

  private static final class MemStoreReader implements BlockEntryIterator {
    private final Iterator<BlockEntry> iterator;

    private MemStoreReader(final ConcurrentSkipListSet<BlockEntry> entries) {
      this.iterator = entries.iterator();
    }

    private MemStoreReader(final ConcurrentSkipListSet<BlockEntry> entries, final ByteArraySlice key) {
      this.iterator = entries.tailSet(new BlockEntry().setKey(key), true).iterator();
    }

    @Override
    public void close() {
      // no-op
    }

    @Override
    public boolean hasMoreEntries() throws IOException {
      return iterator.hasNext();
    }

    @Override
    public BlockEntry nextEntry() throws IOException {
      return iterator.next();
    }
  }
}
