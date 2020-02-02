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
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.ByteArrayWriter;
import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.bytes.encoding.IntEncoder;
import tech.dnaco.io.FileBufferedOutputStream;
import tech.dnaco.storage.encoding.BlockEncoding;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.telemetry.Histogram;
import tech.dnaco.telemetry.TelemetryCollector;
import tech.dnaco.telemetry.TelemetryCollectorRegistry;

public class DataBlocksWriter implements AutoCloseable {
  private static final Histogram dataBlockFlushTime = new TelemetryCollector.Builder()
    .setName("storage.data_flush_time")
    .setLabel("Storage Data Flush Time")
    .setUnit(HumansUtil.HUMAN_TIME_NANOS)
    .register(new Histogram(Histogram.DEFAULT_DURATION_BOUNDS_NS));

  private static final Histogram dataBlocksCount = new TelemetryCollector.Builder()
    .setName("storage.data_blocks_count")
    .setLabel("Storage Data Blocks count")
    .setUnit(HumansUtil.HUMAN_COUNT)
    .register(new Histogram(Histogram.DEFAULT_COUNT_BOUNDS));

  private static final Histogram dataBlockSize = new TelemetryCollector.Builder()
    .setName("storage.data_block_size")
    .setLabel("Storage Data Block size")
    .setUnit(HumansUtil.HUMAN_SIZE)
    .register(new Histogram(Histogram.DEFAULT_SIZE_BOUNDS));

  private final ArrayList<BlockInfo> blocks = new ArrayList<>(64);
  private final FileBufferedOutputStream stream;
  private final BlockWriter blockWriter;
  private final BlockStats stats;
  private final long startTime;
  
  private final byte[] firstKey;
  private int firstKeyLen;

  private static final byte[] FORMAT_TYPE = new byte[] { 'D', 'A', 'T', 1 };

  public DataBlocksWriter(final File file, final BlockStats stats) throws IOException {
    this.startTime = System.nanoTime();

    this.stream = new FileBufferedOutputStream(file);
    this.stats = stats;
    this.blockWriter = new BlockWriter(stats.getKeyMaxLength());

    this.firstKey = new byte[stats.getKeyMaxLength()];
    this.firstKeyLen = 0;

    // format
    stream.write(FORMAT_TYPE);
    stats.writeTo(stream); // ???
  }

  @Override
  public void close() throws IOException {
    if (blockWriter.hasData()) {
      flushBlock();
    }

    // write block Index
    writeBlocksIndex();
    writeTrailer();

    stream.flush();
    stream.close();

    dataBlocksCount.add(blocks.size());
    dataBlockSize.add(stream.position());
    dataBlockFlushTime.add(System.nanoTime() - startTime);
  }

  private void writeBlocksIndex() throws IOException {
    int indexSize = 8;
    for (final BlockInfo blk: blocks) {
      indexSize += 8 + 8 + 8 + blk.firstKey.length + blk.lastKey.length;
    }

    try (ByteArrayWriter indexWriter = new ByteArrayWriter(new byte[indexSize])) {
      indexSize = IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(indexWriter, blocks.size());
      for (final BlockInfo blk: blocks) {
        indexSize += IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(indexWriter, blk.offset);
        indexSize += IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(indexWriter, blk.firstKey.length);
        indexSize += IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(indexWriter, blk.lastKey.length);
        indexWriter.write(blk.firstKey);
        indexWriter.write(blk.lastKey);
        indexSize += blk.firstKey.length + blk.lastKey.length;
      }

      // write compressed index
      indexSize = BlockEncoding.encode(stream, indexWriter);
      IntEncoder.BIG_ENDIAN.writeFixed32(stream, indexSize);
      System.out.println(" -> WRITE BLOCK INDEX " + blocks.size() + " -> " + indexSize);
    }
  }

  private void writeTrailer() {

  }

  public void add(final BlockEntry entry) throws IOException {
    //entry.setSeqId(entry.getSeqId() - stats.getSeqIdMin());
    //entry.setTimestamp(entry.getTimestamp() - stats.getTimestampMin());

    if (!blockWriter.hasSpace(entry)) {
      flushBlock();
    }

    if (!blockWriter.hasData()) {
      firstKeyLen = entry.getKey().length();
      System.arraycopy(entry.getKey().rawBuffer(), entry.getKey().offset(), firstKey, 0, firstKeyLen);
      blocks.add(new BlockInfo(firstKey, firstKeyLen, stream.position()));
    }

    blockWriter.add(entry);
  }

  private void flushBlock() throws IOException {
    final ByteArraySlice lastKey = blockWriter.getLastKey();
    blocks.get(blocks.size() - 1).setLastKey(lastKey);
    blockWriter.flush(stream);
  }

  public static final class BlockInfo {
    private final byte[] firstKey;
    private byte[] lastKey;
    private final long offset;

    public BlockInfo(final byte[] firstKey, final int firstKeyLen, final long offset) {
      this(firstKey, firstKeyLen, null, 0, offset);
    }

    public BlockInfo(final byte[] firstKey, final int firstKeyLen,
        final byte[] lastKey, final int lastKeyLen, final long offset) {
      this.firstKey = Arrays.copyOf(firstKey, firstKeyLen);
      this.lastKey = (lastKey != null) ? Arrays.copyOf(lastKey, lastKeyLen) : null;
      this.offset = offset;
    }

    private void setLastKey(final ByteArraySlice key) {
      this.lastKey = Arrays.copyOf(key.rawBuffer(), key.length());
    }

    public long getOffset() {
      return offset;
    }

    public byte[] firstKey() {
      return firstKey;
    }

    @Override
    public String toString() {
      return "BlockInfo [firstKey=" + new String(firstKey) + ", offset=" + offset + "]";
    }
  }

  public static void main(final String[] args) throws Exception {
    try (DataInputStream stream = new DataInputStream(new BufferedInputStream(new FileInputStream("lega-tabgen.kv")))) {
      final long prepareTime = System.nanoTime();
      final BlockStats stats = new BlockStats();
      stats.update(new BlockEntry()
            .setTimestamp(System.currentTimeMillis())
            .setSeqId(1)
            .setFlags(1)
            .setKey(new ByteArraySlice(new byte[4096]))
            .setValue(new ByteArraySlice()));

      final MessageDigest digest = MessageDigest.getInstance("SHA-1");
      final List<BlockEntry> entries = new ArrayList<>(1_000_000);
      while (stream.available() > 0) {
        final String key = stream.readUTF();
        final String value = stream.readUTF();
        final BlockEntry entry = new BlockEntry()
          .setTimestamp(System.currentTimeMillis())
          .setSeqId(1)
          .setFlags(1)
          .setKey(new ByteArraySlice(key.getBytes()))
          .setValue(new ByteArraySlice(value.getBytes()));
        entries.add(entry);
        entry.hash(digest);
      }
      System.out.println(" -> [T] Prepare " + HumansUtil.humanTimeSince(prepareTime) + " ROWS " + entries.size());
      System.out.println(" -> HASH " + BytesUtil.toHexString(digest.digest()));

      if (true) {
        final long writeTime = System.nanoTime();
        try (DataBlocksWriter writer = new DataBlocksWriter(new File("lega-tabgen.block"), stats)) {
          for (final BlockEntry entry: entries) {
            writer.add(entry);
          }
        }
        System.out.println(" -> [T] Writer " + HumansUtil.humanTimeSince(writeTime));
      }
    }

    TelemetryCollectorRegistry.INSTANCE.humanReport();
  }
}
