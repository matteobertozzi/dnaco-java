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
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import tech.dnaco.bytes.ByteArrayReader;
import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.encoding.IntDecoder;
import tech.dnaco.io.ByteBufferInputStream;
import tech.dnaco.logging.Logger;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.telemetry.Histogram;
import tech.dnaco.telemetry.TelemetryCollector;
import tech.dnaco.telemetry.TelemetryCollectorRegistry;

public class DataBlocksReader implements AutoCloseable, BlockEntryIterator {
  private static final Histogram dataIndexTime = new TelemetryCollector.Builder()
    .setName("storage.index_read_time")
    .setLabel("Storage Index Read Time")
    .setUnit(HumansUtil.HUMAN_TIME_NANOS)
    .register(new Histogram(Histogram.DEFAULT_DURATION_BOUNDS_NS));

  private static final Histogram dataSeekTime = new TelemetryCollector.Builder()
    .setName("storage.index_seek_time")
    .setLabel("Storage Index Seek Time")
    .setUnit(HumansUtil.HUMAN_TIME_NANOS)
    .register(new Histogram(Histogram.DEFAULT_DURATION_BOUNDS_NS));

  private final BlockReader blockReader = new BlockReader();
  private final BlockStats stats = new BlockStats();
  private final MappedByteBuffer mappedBuffer;
  private final ByteBufferInputStream stream;
  private final RandomAccessFile raf;
  private final BlockInfo[] blocks;

  private final File file;

  @Override
  public String toString() {
    return "DataReader f=" + file;
  }

  public DataBlocksReader(final File file) throws IOException {
    this(file, null);
  }

  public DataBlocksReader(final File file, final BlockInfo[] blockIndex) throws IOException {
    this.file = file;
    this.raf = new RandomAccessFile(file, "r");
    final long length = raf.length();
    this.mappedBuffer = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, length);
    this.stream = new ByteBufferInputStream(mappedBuffer, length);
    mappedBuffer.load();

    final byte[] format = this.stream.readNBytes(4);
    if (format[0] != 'D' || format[1] != 'A' || format[2] != 'T' || format[3] != 1) {
      throw new IllegalArgumentException("invalid format " + new String(format, 0, 3) + " -> " + format[3]);
    }

    // read stats
    this.stats.readFrom(this.stream);

    // read index
    this.blocks = (blockIndex != null) ? blockIndex : readBlockIndex(stream);
    //System.out.println("BLOCKS: " + Arrays.toString(blocks));
  }

  public BlockStats getStats() {
    return stats;
  }

  public BlockInfo[] getBlocks() {
    return blocks;
  }

  private static BlockInfo[] readBlockIndex(final ByteBufferInputStream stream) throws IOException {
    final long startTime = System.nanoTime();
    stream.seekTo(stream.length() - 4);
    Logger.debug("Seek to: {}", stream.length() - 4);

    final int indexSize = IntDecoder.BIG_ENDIAN.readFixed32(stream);
    Logger.debug("index size: {}", indexSize);
    stream.seekTo(stream.length() - (indexSize + 4));

    final byte[] indexBlock = BlockEncoding.decode(stream);
    try (ByteArrayReader indexReader = new ByteArrayReader(indexBlock)) {
      final int indexEntries = IntDecoder.BIG_ENDIAN.readUnsignedVarInt(indexReader);
      final BlockInfo[] blocks = new BlockInfo[indexEntries];
      for (int i = 0; i < indexEntries; ++i) {
        final int blockOffset = IntDecoder.BIG_ENDIAN.readUnsignedVarInt(indexReader);
        final int firstKeyLen = IntDecoder.BIG_ENDIAN.readUnsignedVarInt(indexReader);
        final int lastKeyLen = IntDecoder.BIG_ENDIAN.readUnsignedVarInt(indexReader);
        final byte[] firstKey = indexReader.readNBytes(firstKeyLen);
        final byte[] lastKey = indexReader.readNBytes(lastKeyLen);
        blocks[i] = new BlockInfo(firstKey, firstKeyLen, lastKey, lastKeyLen, blockOffset);
        //System.out.println(" -> " + i + " -> " + blockOffset + " -> " + firstKeyLen + " -> " + lastKeyLen + " -> " + blocks[i]);
      }

      final long elapsed = System.nanoTime() - startTime;
      dataIndexTime.add(elapsed);
      System.out.println(" -> Load Index Time: " + HumansUtil.humanTimeNanos(elapsed));
      return blocks;
    }
  }

  @Override
  public void close() throws IOException {
    stream.close();
    raf.close();
  }

  public BlockReader getBlockReader(final BlockInfo blockInfo) throws IOException {
    this.blockIndex = 0;
    for (int i = 0, n = blocks.length; i < n; ++i) {
      if (blocks[i] == blockInfo) {
        this.blockIndex = i + 1;
        break;
      }
    }

    //System.out.println(" READ ---> " + this + " -> " + blockIndex + "/" + blocks.length + " -> " + blockInfo);
    stream.seekTo(blockInfo.getOffset());
    blockReader.read(stream);
    return blockReader;
  }

  private int blockIndex = 0;

  @Override
  public boolean hasMoreEntries() throws IOException {
    if (blockReader.hasMoreEntries()) return true;
    if (blockIndex >= blocks.length) return false;

    getBlockReader(blocks[blockIndex++]);
    return blockReader.hasMoreEntries();
  }

  @Override
  public BlockEntry nextEntry() throws IOException {
    return adjustEntry(blockReader.nextEntry());
  }

  public BlockEntry adjustEntry(final BlockEntry entry) {
    entry.setSeqId(entry.getSeqId() + stats.getSeqIdMin());
    entry.setTimestamp(entry.getTimestamp() + stats.getTimestampMin());
    return entry;
  }


  public boolean seekTo(final ByteArraySlice key) throws IOException {
    final long startTime = System.nanoTime();
    final BlockInfo blockInfo =  BlockInfo.seekTo(blocks, key);
    if (blockInfo != null) {
      getBlockReader(blockInfo);
      blockReader.seekTo(key);
    } else {
      getBlockReader(blocks[0]);
    }
    dataSeekTime.add(System.nanoTime() - startTime);
    return blockInfo != null;
  }

  public void seekToFirst() throws IOException {
    getBlockReader(blocks[0]);
  }

  public static void main(final String[] args) throws Exception {
    for (int i = 0; i < args.length; ++i) {
      final long startTime = System.nanoTime();
      try (DataBlocksReader reader = new DataBlocksReader(new File(args[i]))) {
        int blockCount = 0;
        int rowCount = 0;

        for (final BlockInfo blockInfo: reader.getBlocks()) {
          final BlockReader block = reader.getBlockReader(blockInfo);
          System.out.println(blockCount + " - " + block.getStats());
          while (block.hasMoreEntries()) {
            final BlockEntry entry = block.nextEntry();
            reader.adjustEntry(entry);
            System.out.println(" -> " + rowCount + " -> " + entry);
            rowCount++;
          }
          blockCount++;
        }
        System.out.println(" -> BLOCKS " + blockCount + " ROWS " + rowCount);
      }
      System.out.println(" ---> " + HumansUtil.humanTimeSince(startTime));
    }

    TelemetryCollectorRegistry.INSTANCE.humanReport();
  }
}
