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

import java.security.MessageDigest;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.encoding.IntEncoder;

public class BlockEntry {
  public static final long FLAG_INSERT = 1 << 0;
  public static final long FLAG_UPDATE = 1 << 1;
  public static final long FLAG_UPSERT = 1 << 2;
  public static final long FLAG_DELETE = 1 << 3;

  private long seqId;
  private long timestamp;
  private long flags;
  private ByteArraySlice key;
  private ByteArraySlice value;

  public int estimateSize() {
    return 16 + 8 + 8 + 4 + key.length() + value.length();
  }

  public byte[] keySha1() {
    try {
      final MessageDigest msdDigest = MessageDigest.getInstance("SHA-1");
      msdDigest.update(key.rawBuffer(), key.offset(), key.length());
      return msdDigest.digest();
    } catch (final Throwable e) {
      throw new RuntimeException(e);
    }
  }

  public long getSeqId() {
    return seqId;
  }

  public BlockEntry setSeqId(final long seqId) {
    this.seqId = seqId;
    return this;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public BlockEntry setTimestamp(final long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  public long getFlags() {
    return flags;
  }

  public BlockEntry setFlags(final long flags) {
    this.flags = flags;
    return this;
  }

  public ByteArraySlice getKey() {
    return key;
  }

  public BlockEntry setKey(final ByteArraySlice key) {
    this.key = key;
    return this;
  }

  public ByteArraySlice getValue() {
    return value;
  }

  public BlockEntry setValue(final ByteArraySlice value) {
    this.value = value;
    return this;
  }

  public void hash(final MessageDigest digest) {
    final byte[] buf = new byte[8];
    IntEncoder.BIG_ENDIAN.writeFixed64(buf, 0, seqId);
    digest.update(buf, 0, 8);
    IntEncoder.BIG_ENDIAN.writeFixed64(buf, 0, timestamp);
    digest.update(buf, 0, 8);
    IntEncoder.BIG_ENDIAN.writeFixed64(buf, 0, flags);
    digest.update(buf, 0, 8);
    digest.update(key.rawBuffer(), key.offset(), key.length());
    digest.update(value.rawBuffer(), value.offset(), value.length());
  }

  @Override
  public String toString() {
    return "BlockEntry [seqId=" + seqId + ", timestamp=" + timestamp + ", flags=" + flags +
        ", key=" + key + ", value=" + value + "]";
  }

  public static int compareKey(final BlockEntry a, final BlockEntry b) {
    return a.getKey().compareTo(b.getKey());
  }

  public static int compare(final BlockEntry a, final BlockEntry b) {
    final int cmp = a.getKey().compareTo(b.getKey());
    //System.out.println("CMP " + cmp + " -> " + a.seqId + " -> " + b.seqId);
    return cmp != 0 ? cmp : Long.compare(a.getSeqId(), b.getSeqId());
  }
}
