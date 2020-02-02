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
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.util.Arrays;

import tech.dnaco.bytes.ByteArrayReader;
import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.bytes.encoding.IntDecoder;
import tech.dnaco.io.ByteBufferInputStream;
import tech.dnaco.storage.block.DataBlocksWriter.BlockInfo;
import tech.dnaco.storage.encoding.BlockEncoding;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.telemetry.TelemetryCollectorRegistry;

public class DataBlocksReader implements AutoCloseable, BlockEntryIterator {    
  private final BlockReader blockReader = new BlockReader();
  private final BlockStats stats = new BlockStats();
  private final MappedByteBuffer mappedBuffer;
  private final ByteBufferInputStream stream;
  private final RandomAccessFile raf;
  private final BlockInfo[] blocks;
  
  private final File file;

  @Override
  public String toString() {
    return "DataReader f=" + file + " blk=" + blockIndex;
  }

  public DataBlocksReader(final File file) throws IOException {
    this.file = file;
    this.raf = new RandomAccessFile(file, "r");
    final long length = raf.length();
    this.mappedBuffer = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, length);
    this.stream = new ByteBufferInputStream(mappedBuffer, length);
    mappedBuffer.load();

    // TODO: read index for lookups?
    final byte[] format = this.stream.readNBytes(4);
    System.out.println(new String(format, 0, 3) + " -> " + format[3]);

    // read stats
    this.stats.readFrom(this.stream);

    // read index
    this.blocks = readBlockIndex(stream);
    //System.out.println("BLOCKS: " + Arrays.toString(blocks));
  }

  public BlockInfo[] getBlocks() {
    return blocks;
  }

  public BlockInfo seekTo(final byte[] key) {
    int low = 0;
    int high = blocks.length - 1;

    while (low <= high) {
        final int mid = (low + high) >>> 1;
        final BlockInfo midVal = blocks[mid];
        final int cmp = Arrays.compare(midVal.firstKey(), key);

        if (cmp < 0)
            low = mid + 1;
        else if (cmp > 0)
            high = mid - 1;
        else
            return midVal; // key found
    }
    //return -(low + 1);  // key not found.
    System.out.println("LOW " + low + " HIGH " + high);
    return high < 0 ? null : blocks[high];
  }

  private static BlockInfo[] readBlockIndex(final ByteBufferInputStream stream) throws IOException {
    stream.seekTo(stream.length() - 4);

    final int indexSize = IntDecoder.BIG_ENDIAN.readFixed32(stream);
    System.out.println("INDEX SIZE " + indexSize);
    stream.seekTo(stream.length() - (indexSize + 4));

    final byte[] indexBlock = BlockEncoding.decode(stream);
    try (ByteArrayReader indexReader = new ByteArrayReader(indexBlock)) {
      final int indexEntries = IntDecoder.BIG_ENDIAN.readUnsignedVarInt(indexReader);
      final BlockInfo[] blocks = new BlockInfo[indexEntries];
      System.out.println("INDEX ENTRIES " + indexEntries);
      for (int i = 0; i < indexEntries; ++i) {
        final int blockOffset = IntDecoder.BIG_ENDIAN.readUnsignedVarInt(indexReader);
        final int firstKeyLen = IntDecoder.BIG_ENDIAN.readUnsignedVarInt(indexReader);
        final int lastKeyLen = IntDecoder.BIG_ENDIAN.readUnsignedVarInt(indexReader);
        final byte[] firstKey = indexReader.readNBytes(firstKeyLen);
        final byte[] lastKey = indexReader.readNBytes(lastKeyLen);
        blocks[i] = new BlockInfo(firstKey, firstKeyLen, lastKey, lastKeyLen, blockOffset);
        //System.out.println(" -> " + i + " -> " + blockOffset + " -> " + firstKeyLen + " -> " + lastKeyLen + " -> " + blocks[i]);
      }
      return blocks;
    }
  }

  @Override
  public void close() throws IOException {
    stream.close();
    raf.close();
  }

  public BlockReader getBlockReader(final BlockInfo blockInfo) throws IOException {
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
    //entry.setSeqId(entry.getSeqId() + stats.getSeqIdMin());
    //entry.setTimestamp(entry.getTimestamp() + stats.getTimestampMin());
    return entry;
  }

  public static void main(final String[] args) throws Exception {
    final long startTime = System.nanoTime();
    try (DataBlocksReader reader = new DataBlocksReader(new File("lega-tabgen.block"))) {
      int blockCount = 0;
      int rowCount = 0;

      if (false) {
        final byte[] key = "LSA_X".getBytes();
        final BlockInfo blockX = reader.seekTo(key);
        System.out.println(blockX);

        final BlockReader block = reader.getBlockReader(blockX);
        block.seekTo(new ByteArraySlice(key));
        //System.out.println(blockCount + " - " + block.getStats());
        while (false && block.hasMoreEntries()) {
          final BlockEntry entry = block.nextEntry();
          if (entry.getKey().compareTo(new ByteArraySlice(key)) == 0) {
            System.out.println(" -> " + entry);
          }
          rowCount++;
        }
        blockCount++;
      } else {
        final MessageDigest digest = MessageDigest.getInstance("SHA-1");
        for (final BlockInfo blockInfo: reader.getBlocks()) {
          final BlockReader block = reader.getBlockReader(blockInfo);
          //System.out.println(blockCount + " - " + block.getStats());
          while (block.hasMoreEntries()) {
            final BlockEntry entry = block.nextEntry();
            //reader.adjustEntry(entry);
            //System.out.println(" -> " + rowCount + " -> " + entry);
            entry.hash(digest);
            rowCount++;
          }
          blockCount++;
        }
        System.out.println(" -> HASH " + BytesUtil.toHexString(digest.digest()));
      }
      System.out.println(" -> BLOCKS " + blockCount + " ROWS " + rowCount);
      // b5110de0a352700f4cb28127c2951f1154d42f6f
    }
    System.out.println(" ---> " + HumansUtil.humanTimeSince(startTime));

    TelemetryCollectorRegistry.INSTANCE.humanReport();
  }
}
