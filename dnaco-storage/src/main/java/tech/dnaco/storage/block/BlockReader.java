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

import java.io.IOException;
import java.io.InputStream;

import tech.dnaco.bytes.ByteArrayReader;
import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.BytesSlice;
import tech.dnaco.bytes.encoding.IntDecoder;
import tech.dnaco.collections.ArrayUtil;
import tech.dnaco.io.ByteBufferInputStream;
import tech.dnaco.storage.encoding.BitDecoder;
import tech.dnaco.storage.encoding.BlockEncoding;
import tech.dnaco.storage.encoding.DeltaByteDecoder;
import tech.dnaco.storage.encoding.IntArrayDecoder;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.telemetry.Histogram;
import tech.dnaco.telemetry.TelemetryCollector;

public class BlockReader implements BlockEntryIterator {
  private static final Histogram blockLoadTime = new TelemetryCollector.Builder()
    .setName("storage.block_load_time")
    .setLabel("Storage Block Load Time")
    .setUnit(HumansUtil.HUMAN_TIME_NANOS)
    .register(new Histogram(Histogram.DEFAULT_DURATION_BOUNDS_NS));
    
  private static final IntDecoder INT_DECODER = IntDecoder.BIG_ENDIAN;

  private final HashIndexReader hashIndex = new HashIndexReader();
  private final BlockStats stats = new BlockStats();
  private final BlockEntry entry = new BlockEntry()
      .setKey(new ByteArraySlice())
      .setValue(new ByteArraySlice());

  private DeltaByteDecoder keyDeltaDecoder;
  private ByteArrayReader block;
  private int[] restartsIndex;
  private int rowIndex;
  private byte[] value;
  private byte[] key;

  public BlockStats getStats() {
    return stats;
  }

  @Override
  public boolean hasMoreEntries() {
    return rowIndex < stats.getRowCount();
  }

  @Override
  public BlockEntry nextEntry() throws IOException {
    readEntry(rowIndex++);
    return entry;
  }

  public int seekTo(final int offset, int length, final BytesSlice key) throws IOException {
    block.seekTo(offset);
    keyDeltaDecoder.reset();
    int cmp = 1;
    while (length > 0) {
      readEntry(0);

      cmp = entry.getKey().compareTo(key);
      if (cmp < 0) {
        System.out.println("SEEK TO " + offset + " -> " + key + " -> " + entry);
        return cmp;
      }
      length -= block.readOffset() - offset;
    }
    return cmp;
  }

  public int seekTo(final BytesSlice key) throws IOException {
    int low = 0;
    int high = restartsIndex.length - 1;

    while (low <= high) {
        final int mid = (low + high) >>> 1;
        final int midVal = restartsIndex[mid];
        final int cmp = seekTo(midVal, (mid + 1) < restartsIndex.length ? restartsIndex[mid + 1] - midVal : block.length() - midVal, key);
        System.out.println("MID " + mid + " " + restartsIndex.length + " CMP " + cmp);

        if (cmp < 0)
            low = mid + 1;
        else if (cmp > 0)
            high = mid - 1;
        else
            return mid; // key found
    }
    //return -(low + 1);  // key not found.
    System.out.println("LOW " + low + " HIGH " + high);
    return high;
  }

  private void readEntry(final int rowIndex) throws IOException {
    entry.setSeqId(INT_DECODER.readUnsignedVarLong(block));
    entry.setTimestamp(INT_DECODER.readUnsignedVarLong(block));
    entry.setFlags(INT_DECODER.readUnsignedVarLong(block));
    final int keyShared = INT_DECODER.readUnsignedVarInt(block);
    final int keyUnshared = INT_DECODER.readUnsignedVarInt(block);
    final int valLength = INT_DECODER.readUnsignedVarInt(block);
    System.arraycopy(keyDeltaDecoder.rawBuffer(), 0, key, 0, keyShared);
    block.readNBytes(key, keyShared, keyUnshared);
    block.readNBytes(value, 0, valLength);
    entry.getKey().set(key, 0, keyShared + keyUnshared);
    entry.getValue().set(value, 0, valLength);
    keyDeltaDecoder.add(entry.getKey());
  }

  public boolean read(final ByteBufferInputStream stream) throws IOException {
    if (stream.available() == 0) return false;

    final long startTime = System.nanoTime();

    final int magic = INT_DECODER.readFixed32(stream);
    if (magic != 0xD474B10C) {
      throw new IOException("Invalid Data Block Magic. Expected 0xD474B10C got " + Integer.toHexString(magic));
    }

    // read stats
    this.stats.readFrom(stream);

    // prepare reader buffers
    this.keyDeltaDecoder = new DeltaByteDecoder(stats.getKeyMaxLength());
    this.key = ArrayUtil.newIfNotAtSize(this.key, stats.getKeyMaxLength());
    this.value = ArrayUtil.newIfNotAtSize(this.value, stats.getValueMaxLength());
    this.rowIndex = 0;

    // read block data
    final byte[] blockData = BlockEncoding.decode(stream);
    this.block = new ByteArrayReader(blockData);

    // read indexes
    restartsIndex = IntArrayDecoder.decodeSequence(stream);
    hashIndex.readIndex(stream);

    final long loadTime = System.nanoTime() - startTime;
    blockLoadTime.add(loadTime);
    return true;
  }

  private static final class HashIndexReader {
    private int[] buckets;

    public void readIndex(final InputStream stream) throws IOException {
      final int bucketsLen = INT_DECODER.readUnsignedVarInt(stream);
      final int zeroCount = INT_DECODER.readUnsignedVarInt(stream);
      final int maxWidth = INT_DECODER.readUnsignedVarInt(stream);
      final int valueLength = bucketsLen - zeroCount;

      buckets = ArrayUtil.newIfNotAtSize(buckets, bucketsLen);
      final BitDecoder bitmap = new BitDecoder(stream, 2);
      for (int i = 0; i < bucketsLen; ++i) {
        final long v = bitmap.read();
        buckets[i] = (v < 2) ? (int) v : -1;
      }

      int index = 0;
      final BitDecoder data = new BitDecoder(stream, maxWidth);
      for (int i = 0; i < valueLength; ++i) {
        index = ArrayUtil.indexOf(buckets, index, buckets.length - index, -1);
        buckets[index] = (int)(2 + data.read());
      }
    }
  }
}
