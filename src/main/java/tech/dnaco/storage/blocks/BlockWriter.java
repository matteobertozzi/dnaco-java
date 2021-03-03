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

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.ByteArrayWriter;
import tech.dnaco.bytes.encoding.IntEncoder;
import tech.dnaco.bytes.encoding.IntUtil;
import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.collections.arrays.IntArray;
import tech.dnaco.collections.arrays.LongArray;
import tech.dnaco.data.hashes.XXHash;
import tech.dnaco.storage.encoding.BitEncoder;
import tech.dnaco.storage.encoding.DeltaBytesEncoder;
import tech.dnaco.storage.encoding.IntArrayEncoder;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.telemetry.Histogram;
import tech.dnaco.telemetry.TelemetryCollector;
import tech.dnaco.util.BitUtil;


// |111|111|--| |11|11|11|11| |11|11|11|11|
// +-------------------------+-------------------------+
// |           SeqId         |       Timestamp         |
// +------------+------------+------------+------------+
// | Row Flags  |   K-Hash   |            |            |
// +------------+------------+------------+------------+
// |  K-Shared  | K-Unshared |  V-Shared  | V-Unshared |
// +------------+------------+------------+------------+
// |                    Key Unshared                   |
// +---------------------------------------------------+
// |                   Value Unshared                  |
// +---------------------------------------------------+
public class BlockWriter {
  private static final Histogram blockFlushTime = new TelemetryCollector.Builder()
    .setName("storage.block_flush_time")
    .setLabel("Storage Block Flush Time")
    .setUnit(HumansUtil.HUMAN_TIME_NANOS)
    .register(new Histogram(Histogram.DEFAULT_DURATION_BOUNDS_NS));

  private static final Histogram blockPlainSize = new TelemetryCollector.Builder()
    .setName("storage.block_plain_size")
    .setLabel("Storage Block Plain Size")
    .setUnit(HumansUtil.HUMAN_SIZE)
    .register(new Histogram(Histogram.DEFAULT_SIZE_BOUNDS));

  private static final IntEncoder INT_ENCODER = IntEncoder.BIG_ENDIAN;

  private final ByteArrayWriter writer = new ByteArrayWriter(new byte[1 << 20]);
  private final DeltaBytesEncoder keyDeltaEncoder;

  private final HashIndexWriter hashIndex = new HashIndexWriter();
  private final IntArray index = new IntArray(1 << 10);
  private final BlockStats stats = new BlockStats();
  private int restartBlockSize = Integer.MAX_VALUE;

  public BlockWriter(final int maxKeyLength) {
    this.keyDeltaEncoder = new DeltaBytesEncoder(maxKeyLength);
  }

  public boolean hasData() {
    return writer.available() != writer.length();
  }

  public boolean hasSpace(final BlockEntry entry) {
    return entry.estimateSize() < writer.available();
  }

  public ByteArraySlice getLastKey() {
    return keyDeltaEncoder.getValue();
  }

  public void add(final BlockEntry entry) throws IOException {
    final int writerInitialSize = writer.writeOffset();
    if (restartBlockSize >= (4 << 10)) {
      index.add(writerInitialSize);
      keyDeltaEncoder.reset();
      restartBlockSize = 0;
    }

    hashIndex.add(entry.getKey(), index.size());

    final int keyShared = keyDeltaEncoder.add(entry.getKey());
    final int keyUnshared = entry.getKey().length() - keyShared;

    INT_ENCODER.writeUnsignedVarLong(writer, entry.getSeqId());
    INT_ENCODER.writeUnsignedVarLong(writer, entry.getTimestamp());
    //INT_ENCODER.writeFixed32(writer, hashCode);
    INT_ENCODER.writeUnsignedVarLong(writer, entry.getFlags());
    INT_ENCODER.writeUnsignedVarLong(writer, keyShared);
    INT_ENCODER.writeUnsignedVarLong(writer, keyUnshared);
    INT_ENCODER.writeUnsignedVarLong(writer, entry.getValue().length());
    writer.write(entry.getKey(), keyShared, keyUnshared);
    //System.out.println(" -> KEY SHARED " + keyShared + " -> UNSHARED " + keyUnshared + " -> " + len);
    writer.write(entry.getValue());

    final int entrySize = writer.writeOffset() - writerInitialSize;
    this.restartBlockSize += entrySize;

    stats.update(entry);
  }

  public void flush(final OutputStream stream) throws IOException {
    final long startTime = System.nanoTime();

    // | MAGIC | Block Stats | Block Data |
    INT_ENCODER.writeFixed32(stream, 0xD474B10C);
    stats.writeTo(stream);
    BlockEncoding.encode(stream, writer);

    if (true) {
      //System.out.println(" -> INDEX " + index.size() + " -> ROW-COUNT " + stats.getRowCount());
      if (index.get(0) != 0) {
        throw new IOException("EXPECTED 0 as FIRST INDEX");
      }
      IntArrayEncoder.encodeSequence(stream, index.rawBuffer(), 1, index.size() - 1);
    }
    if (true) {
      hashIndex.writeIndex(stream);
    }

    final long elapsed = System.nanoTime() - startTime;
    blockFlushTime.add(elapsed);
    blockPlainSize.add(writer.writeOffset());
    //System.out.println("Block Written in " + HumansUtil.humanTimeNanos(elapsed));

    // reset for the next block
    keyDeltaEncoder.reset();
    writer.reset();
    stats.reset();

    restartBlockSize = Integer.MAX_VALUE;
    hashIndex.reset();
    index.reset();
  }

  private static final class HashIndexWriter {
    private final LongArray hashes = new LongArray(16 << 10);

    public void reset() {
      hashes.reset();
    }

    public void add(final ByteArraySlice key, final int index) {
      final long hashCode = XXHash.hash64(XXHash.DEFAULT_SEED_64, ArrayUtil.copyIfNotAtSize(key.rawBuffer(), key.offset(), key.length()));
      add(hashCode, index);
    }

    public void add(final long hashCode, final int index) {
      hashes.add((hashCode << 12L) | index);
    }

    public void writeIndex(final OutputStream stream) throws IOException {
      int collisions = 0;
      int matchCollision = 0;
      final int[] buckets = new int[BitUtil.nextPow2(hashes.size())];
      Arrays.fill(buckets, 0);
      for (int i = 0, n = hashes.size(); i < n; ++i) {
        final long v = hashes.get(i);
        final long hash = (v >>> 12);
        final int restartBlock = 2 + (int) (v & 0xfff);
        final int index = (int) (hash % buckets.length);
        if (buckets[index] == 0) {
          buckets[index] = restartBlock;
        } else if (buckets[index] != restartBlock || buckets[index] == 1) {
          buckets[index] = 1;
          collisions++;
        } else if (buckets[index] == restartBlock) {
          matchCollision++;
        }
      }

      if (false) {
        System.out.println("HASHES " + hashes.size() + " BUCKETS " + buckets.length
          + " -> COLLISIONS " + collisions + String.format(" %.2f", (float)collisions / (float)hashes.size())
          + " -> MATCHES " + matchCollision);
      }
      encodeBuckets(stream, buckets);
    }

    public void encodeBuckets(final OutputStream stream, final int[] buckets)
        throws IOException {
      // compute min/max stats
      int zeroCount = 0;
      int maxWidth = 0;
      for (int i = 0; i < buckets.length; ++i) {
        if (buckets[i] < 2) {
          zeroCount++;
          continue;
        }
        final int v = buckets[i] - 2;
        final int width = IntUtil.getWidth(v);
        maxWidth = Math.max(maxWidth, width);
      }

      final int bitmapLength = buckets.length / 4;
      final int dataLength = (((buckets.length - zeroCount) * maxWidth) + 7) / 8;
      final byte[] buffer = new byte[bitmapLength + dataLength];

      if (false) {
        System.out.println(" -> ZERO COUNT " + zeroCount +
          " VALUES " + (buckets.length - zeroCount) +
          " BIT-WIDTH " + maxWidth +
          " TOTAL SIZE " + HumansUtil.humanSize(buffer.length));
      }

      try (ByteArrayWriter writer = new ByteArrayWriter(new byte[16 + bitmapLength + dataLength])) {
        try (BitEncoder bitmap = new BitEncoder(writer, 2)) {
          for (int i = 0; i < buckets.length; ++i) {
            bitmap.add((buckets[i] < 2) ? buckets[i] : 3);
          }
        }

        try (BitEncoder data = new BitEncoder(writer, maxWidth)) {
          for (int i = 0; i < buckets.length; ++i) {
            if (buckets[i] < 2) continue;
            data.add(buckets[i] - 2);
          }
        }

        INT_ENCODER.writeUnsignedVarLong(stream, buckets.length);
        INT_ENCODER.writeUnsignedVarLong(stream, zeroCount);
        INT_ENCODER.writeUnsignedVarLong(stream, maxWidth);
        stream.write(writer.rawBuffer(), writer.offset(), writer.writeOffset());
      }
    }
  }

  public static void main(final String[] args) throws Exception {
    try (DataInputStream stream = new DataInputStream(new FileInputStream("lega-tabgen.kv"))) {
      final BlockWriter writer = new BlockWriter(4096);
      final int rowCount = 0;
      int blockCount = 0;
      while (stream.available() > 0) {
        final String key = stream.readUTF();
        final String value = stream.readUTF();
        final BlockEntry entry = new BlockEntry()
          .setTimestamp(System.currentTimeMillis())
          .setSeqId(1)
          .setFlags(1)
          .setKey(new ByteArraySlice(key.getBytes()))
          .setValue(new ByteArraySlice(value.getBytes()));
        //rowCount++;

        if (!writer.hasSpace(entry)) {
          writer.flush(OutputStream.nullOutputStream());
          blockCount++;

          if (blockCount == 2) break;
          //break;
        }
        writer.add(entry);
      }
      if (writer.hasData()) {
        writer.flush(OutputStream.nullOutputStream());
        blockCount++;
      }
      System.out.println("ROW-COUNT " + rowCount + " BLOCK-COUNT " + blockCount);
    }
  }
}
