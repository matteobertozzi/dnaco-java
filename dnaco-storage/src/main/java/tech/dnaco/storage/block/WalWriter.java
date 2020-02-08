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
import java.util.List;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.ByteArrayWriter;
import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.bytes.encoding.IntDecoder;
import tech.dnaco.bytes.encoding.IntEncoder;
import tech.dnaco.collections.ArraySortUtil;
import tech.dnaco.collections.ArraySortUtil.IntArrayComparator;
import tech.dnaco.collections.IntArrayList;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.telemetry.Histogram;
import tech.dnaco.telemetry.TelemetryCollector;

public class WalWriter implements AutoCloseable {
  private static final Histogram walPrepareTime = new TelemetryCollector.Builder()
    .setName("storage.wal_prepare_time")
    .setLabel("Storage WAL Prepare Time")
    .setUnit(HumansUtil.HUMAN_TIME_NANOS)
    .register(new Histogram(Histogram.DEFAULT_DURATION_BOUNDS_NS));

  private static final Histogram walFlushTime = new TelemetryCollector.Builder()
    .setName("storage.wal_flush_time")
    .setLabel("Storage WAL Flush Time")
    .setUnit(HumansUtil.HUMAN_TIME_NANOS)
    .register(new Histogram(Histogram.DEFAULT_DURATION_BOUNDS_NS));
  
  private static final Histogram walFlushedEntries = new TelemetryCollector.Builder()
    .setName("storage.wal_flushed_entries")
    .setLabel("Storage WAL Flushed entries")
    .setUnit(HumansUtil.HUMAN_COUNT)
    .register(new Histogram(Histogram.DEFAULT_COUNT_BOUNDS));

  private static final IntEncoder INT_ENCODER = IntEncoder.BIG_ENDIAN;
  private static final IntDecoder INT_DECODER = IntDecoder.BIG_ENDIAN;

  private final BlockManager blockManager;

  private static final int BLOCK_SIZE = 8 << 20;
  private final ByteArrayWriter block = new ByteArrayWriter(new byte[BLOCK_SIZE]);
  private final IntArrayList entryIndex = new IntArrayList(10_000);
  private final BlockStats stats = new BlockStats();

  public WalWriter(final BlockManager blockManager) {
    this.blockManager = blockManager;
  }

  @Override
  public void close() throws IOException {
    if (block.writeOffset() > 0) {
      flush();
    }
  }

  public void append(final BlockEntry entry) throws IOException {
    if (block.available() < entry.estimateSize()) {
      flush();
    }

    entryIndex.add(block.writeOffset());
    stats.update(entry);
    writeEntry(block, entry);
  }

  private void writeEntry(final ByteArrayWriter writer, final BlockEntry entry)
      throws IOException {
    final long seqId = entry.getSeqId() >= 0 ? entry.getSeqId() : blockManager.newEntrySeqId();
    INT_ENCODER.writeFixed32(writer, entry.getKey().length());
    writer.write(entry.getKey());
    INT_ENCODER.writeFixed64(writer, seqId);
    INT_ENCODER.writeFixed64(writer, entry.getTimestamp());
    INT_ENCODER.writeFixed64(writer, entry.getFlags());
    INT_ENCODER.writeFixed32(writer, entry.getValue().length());
    writer.write(entry.getValue());
  }

  private static void readEntry(final BlockEntry entry, final byte[] buf, final int off) {
    final int keyLen = INT_DECODER.readFixed32(buf, off);
    entry.getKey().set(buf, off + 4, keyLen);
    entry.setSeqId(INT_DECODER.readFixed64(buf, off + 4 + keyLen));
    entry.setTimestamp(INT_DECODER.readFixed64(buf, off + 12 + keyLen));
    entry.setFlags(INT_DECODER.readFixed64(buf, off + 20 + keyLen));
    final int valueLen = INT_DECODER.readFixed32(buf, off + 28 + keyLen);
    entry.getValue().set(buf, off + 32 + keyLen, valueLen);
  }

  public void flush() throws IOException {
    try {
      final long startPrepareTime = System.nanoTime();
      ArraySortUtil.sort(entryIndex.rawBuffer(), 0, entryIndex.size(), WAL_ENTRY_COMPARATOR);
      walPrepareTime.add(System.nanoTime() - startPrepareTime);

      final long startTime = System.nanoTime();

      final BlockEntry entry = new BlockEntry();
      entry.setKey(new ByteArraySlice());
      entry.setValue(new ByteArraySlice());

      final File walFile = blockManager.newBlockFile();
      try (final DataBlocksWriter writer = new DataBlocksWriter(walFile, stats)) {
        for (int i = 0, n = entryIndex.size(); i < n; ++i) {
          readEntry(entry, block.rawBuffer(), entryIndex.get(i));
          writer.add(entry);
          if (false) {
            System.out.println(" -> seq=" + entry.getSeqId()
                               + " ts=" + entry.getTimestamp()
                               + " flags=" + entry.getFlags()
                               + " key=" + entry.getKey()
                               + " value=" + entry.getValue());
          }
        }
        
        List<BlockInfo> blocks = writer.writeBlocksIndex();
        blockManager.addBlocks(walFile, blocks);
      }
      final long flushTime = System.nanoTime() - startTime;
      walFlushTime.add(flushTime);
      walFlushedEntries.add(entryIndex.size());

      System.out.println(" --> FLUSH TOOK " + HumansUtil.humanTimeSince(startTime) + " ENTRIES " + entryIndex.size());
    } finally {
      block.reset();
      stats.reset();
      entryIndex.reset();
    }
  }

  private final IntArrayComparator WAL_ENTRY_COMPARATOR = new IntArrayComparator() {
    @Override
    public int compare(final int[] offsets, final int aIndex, final int bIndex) {
      final int aOffset = offsets[aIndex];
      final int bOffset = offsets[bIndex];
      final byte[] buf = block.rawBuffer();

      final int aKeyLen = INT_DECODER.readFixed32(buf, aOffset);
      final int bKeyLen = INT_DECODER.readFixed32(buf, bOffset);

      final int cmp = BytesUtil.compare(buf, aOffset + 4, aKeyLen, buf, bOffset + 4, bKeyLen);
      if (cmp != 0) return cmp;

      final long aSeqId = INT_DECODER.readFixed64(buf, aOffset + 4 + aKeyLen);
      final long bSeqId = INT_DECODER.readFixed64(buf, bOffset + 4 + bKeyLen);
      return Long.compare(bSeqId, aSeqId);
    }
  };
}
