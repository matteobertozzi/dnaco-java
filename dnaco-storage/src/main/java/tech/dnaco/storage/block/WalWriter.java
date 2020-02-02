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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

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
import tech.dnaco.telemetry.TelemetryCollectorRegistry;

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

  private static final AtomicLong WAL_SEQID = new AtomicLong(0);

  private static final int BLOCK_SIZE = 8 << 20;
  private final ByteArrayWriter block = new ByteArrayWriter(new byte[BLOCK_SIZE]);
  private final IntArrayList entryIndex = new IntArrayList(10_000);
  private final BlockStats stats = new BlockStats();

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

  private static void writeEntry(final ByteArrayWriter writer, final BlockEntry entry)
      throws IOException {
    final long seqId = entry.getSeqId() >= 0 ? entry.getSeqId() : WAL_SEQID.incrementAndGet();
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

      final File walFile = new File(String.format("DNAdb/%020d.wal", WAL_SEQID.incrementAndGet()));
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

  public static void main(final String[] args) throws Exception {
    try (DataInputStream stream = new DataInputStream(new BufferedInputStream(new FileInputStream("lega-tabgen.kv")))) {
      final long prepareTime = System.nanoTime();
      final BlockStats stats = new BlockStats();
      stats.update(new BlockEntry()
            .setKey(new ByteArraySlice())
            .setValue(new ByteArraySlice()));

      final List<BlockEntry> entries = new ArrayList<>(1_000_000);
      while (stream.available() > 0) {
        final String key = stream.readUTF();
        final String value = stream.readUTF();
        final BlockEntry entry = new BlockEntry()
          .setTimestamp(System.currentTimeMillis())
          .setSeqId(entries.size())
          .setFlags(BlockEntry.FLAG_UPSERT)
          .setKey(new ByteArraySlice(key.getBytes()))
          .setValue(new ByteArraySlice(value.getBytes()));
        entries.add(entry);
      }
      System.out.println(" -> [T] Prepare " + HumansUtil.humanTimeSince(prepareTime) + " ROWS " + entries.size());
      Collections.shuffle(entries);

      if (true) {
        final long writeTime = System.nanoTime();
        try (WalWriter writer = new WalWriter()) {
          for (final BlockEntry entry: entries) {
            writer.append(entry);
          }
        }
        System.out.println(" -> [T] Writer " + HumansUtil.humanTimeSince(writeTime));
      }
    }
    
    TelemetryCollectorRegistry.INSTANCE.humanReport();
  }
}
