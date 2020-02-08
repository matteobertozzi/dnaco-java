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

import java.util.Arrays;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.BytesUtil;

public final class BlockInfo {
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

  void setLastKey(final ByteArraySlice key) {
    this.lastKey = Arrays.copyOf(key.rawBuffer(), key.length());
  }

  public long getOffset() {
    return offset;
  }

  public byte[] getFirstKey() {
    return firstKey;
  }

  public byte[] getLastKey() {
    return lastKey;
  }

  public static BlockInfo seekTo(final BlockInfo[] blocks, final ByteArraySlice key) {
    int low = 0;
    int high = blocks.length - 1;

    while (low <= high) {
      final int mid = (low + high) >>> 1;
      final BlockInfo midVal = blocks[mid];
      final byte[] blockFirstKey = midVal.getFirstKey();
      final int cmp = BytesUtil.compare(blockFirstKey, 0, blockFirstKey.length, key.rawBuffer(), key.offset(), key.length());
      if (cmp < 0) {
        low = mid + 1;
      } else if (cmp > 0) {
        high = mid - 1;
      } else {
        return midVal; // key found
      }
    }
    return high < 0 ? null : blocks[high];
  }

  @Override
  public String toString() {
    return "BlockInfo [firstKey=" + new String(firstKey) + ", offset=" + offset + "]";
  }
}