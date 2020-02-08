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
import java.io.OutputStream;

import tech.dnaco.bytes.ByteArrayReader;
import tech.dnaco.bytes.ByteArrayWriter;
import tech.dnaco.bytes.encoding.IntDecoder;
import tech.dnaco.bytes.encoding.IntEncoder;
import tech.dnaco.strings.HumansUtil;

public class BlockStats {
  private int keyMinLength = Integer.MAX_VALUE;
  private int keyMaxLength = 0;
  private int valueMinLength = Integer.MAX_VALUE;
  private int valueMaxLength = 0;
  private long seqIdMin = Long.MAX_VALUE;
  private long seqIdMax = 0;
  private long timestampMin = Long.MAX_VALUE;
  private long timestampMax = 0;
  private int keySize = 0;
  private int valueSize = 0;
  private int rowCount = 0;

  public int getKeyMinLength() {
    return keyMinLength;
  }

  public int getKeyMaxLength() {
    return keyMaxLength;
  }

  public int getValueMinLength() {
    return valueMinLength;
  }

  public int getValueMaxLength() {
    return valueMaxLength;
  }

  public long getSeqIdMin() {
    return seqIdMin;
  }

  public long getSeqIdMax() {
    return seqIdMax;
  }

  public long getTimestampMin() {
    return timestampMin;
  }

  public long getTimestampMax() {
    return timestampMax;
  }

  public long getKeysSize() {
    return keySize;
  }

  public long getValuesSize() {
    return valueSize;
  }

  public int getRowCount() {
    return rowCount;
  }

  public void reset() {
    this.keyMinLength = Integer.MAX_VALUE;
    this.keyMaxLength = 0;
    this.valueMinLength = Integer.MAX_VALUE;
    this.valueMaxLength = 0;
    this.seqIdMin = Long.MAX_VALUE;
    this.seqIdMax = 0;
    this.timestampMin = Long.MAX_VALUE;
    this.timestampMax = 0;
    this.keySize = 0;
    this.valueSize = 0;
    this.rowCount = 0;
  }

  public void update(final BlockStats stats) {
    keyMinLength = Math.min(keyMinLength, stats.getKeyMinLength());
    keyMaxLength = Math.max(keyMaxLength, stats.getKeyMaxLength());
    valueMinLength = Math.min(valueMinLength, stats.getValueMinLength());
    valueMaxLength = Math.max(valueMaxLength, stats.getValueMaxLength());
    seqIdMin = Math.min(seqIdMin, stats.getSeqIdMin());
    seqIdMax = Math.max(seqIdMax, stats.getSeqIdMax());
    timestampMin = Math.min(timestampMin, stats.getTimestampMin());
    timestampMax = Math.max(timestampMax, stats.getTimestampMax());
    keySize += stats.getKeysSize();
    valueSize += stats.getValuesSize();
    rowCount += stats.getRowCount();
  }

  public void update(final BlockEntry entry) {
    keyMinLength = Math.min(keyMinLength, entry.getKey().length());
    keyMaxLength = Math.max(keyMaxLength, entry.getKey().length());
    valueMinLength = Math.min(valueMinLength, entry.getValue().length());
    valueMaxLength = Math.max(valueMaxLength, entry.getValue().length());
    seqIdMin = Math.min(seqIdMin, entry.getSeqId());
    seqIdMax = Math.max(seqIdMax, entry.getSeqId());
    timestampMin = Math.min(timestampMin, entry.getTimestamp());
    timestampMax = Math.max(timestampMax, entry.getTimestamp());
    keySize += entry.getKey().length();
    valueSize += entry.getValue().length();
    rowCount++;
  }

  public void writeTo(final OutputStream stream) throws IOException {
    try (ByteArrayWriter writer = new ByteArrayWriter(new byte[128])) {
      IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(writer, rowCount);
      IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(writer, keyMinLength);
      IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(writer, keyMaxLength - keyMinLength);
      IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(writer, valueMinLength);
      IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(writer, valueMaxLength - valueMinLength);
      IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(writer, seqIdMin);
      IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(writer, seqIdMax - seqIdMin);
      IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(writer, timestampMin);
      IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(writer, timestampMax - timestampMin);
      IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(writer, keySize);
      IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(writer, valueSize);

      // flush
      IntEncoder.BIG_ENDIAN.writeUnsignedVarLong(stream, writer.length());
      writer.writeTo(stream);
    }
  }

  public void readFrom(final InputStream stream) throws IOException {
    final int length = IntDecoder.BIG_ENDIAN.readUnsignedVarInt(stream);
    final byte[] data = stream.readNBytes(length);
    try (ByteArrayReader reader = new ByteArrayReader(data)) {
      rowCount = IntDecoder.BIG_ENDIAN.readUnsignedVarInt(reader);
      keyMinLength = IntDecoder.BIG_ENDIAN.readUnsignedVarInt(reader);
      keyMaxLength = IntDecoder.BIG_ENDIAN.readUnsignedVarInt(reader) + keyMinLength;
      valueMinLength = IntDecoder.BIG_ENDIAN.readUnsignedVarInt(reader);
      valueMaxLength = IntDecoder.BIG_ENDIAN.readUnsignedVarInt(reader) + valueMinLength;
      seqIdMin = IntDecoder.BIG_ENDIAN.readUnsignedVarLong(reader);
      seqIdMax = IntDecoder.BIG_ENDIAN.readUnsignedVarLong(reader) + seqIdMin;
      timestampMin = IntDecoder.BIG_ENDIAN.readUnsignedVarLong(reader);
      timestampMax = IntDecoder.BIG_ENDIAN.readUnsignedVarLong(reader) + timestampMin;
      keySize = IntDecoder.BIG_ENDIAN.readUnsignedVarInt(reader);
      valueSize = IntDecoder.BIG_ENDIAN.readUnsignedVarInt(reader);
    }
  }

  @Override
  public String toString() {
    return "BlockStats [keyMinLength=" + keyMinLength + ", keyMaxLength=" + keyMaxLength + ", valueMinLength=" + valueMinLength
        + ", valueMaxLength=" + HumansUtil.humanSize(valueMaxLength) + ", seqIdMin=" + seqIdMin + ", seqIdMax=" + seqIdMax + ", timestampMin=" + timestampMin
        + ", timestampMax=" + timestampMax + ", keySize=" + HumansUtil.humanSize(keySize) + ", valueSize=" + HumansUtil.humanSize(valueSize) + ", rowCount=" + rowCount + "]";
  }
}
