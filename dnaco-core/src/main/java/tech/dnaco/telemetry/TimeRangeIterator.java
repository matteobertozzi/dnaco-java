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

package tech.dnaco.telemetry;

public abstract class TimeRangeIterator implements Comparable<TimeRangeIterator> {
  protected static final int DEFAULT_NBLOCK = 86400;

  private final int nBlocks;
  private final long window;

  private long limitTimestamp = Long.MAX_VALUE;
  private long blockTsOffset = -1;
  private int offset;

  protected TimeRangeIterator(final int nBlocks, final long window) {
    this.nBlocks = nBlocks;
    this.window = window;
  }

  public long window() { return window; }
  public long getTimestamp() { return blockTsOffset + (offset * window); }

  protected long blockTsOffset() { return blockTsOffset; }
  protected int nBlocks() { return nBlocks; }
  protected int offset() { return offset; }

  public void setLimit(final long timestamp) {
    this.limitTimestamp = timestamp;
  }

  public void unsetLimit() {
    this.limitTimestamp = Long.MAX_VALUE;
  }

  public boolean seek(final long timestamp) {
    if (!load(blockTsOffset(nBlocks, window, timestamp))) {
      return false;
    }

    final long alignedTs = (timestamp - (timestamp % window));
    this.offset = (int) ((alignedTs - blockTsOffset) / window);
    return true;
  }

  public boolean hasMore() {
    if (limitTimestamp < getTimestamp()) return false;
    if (offset < nBlocks) return true;
    return load(blockTsOffset + (nBlocks * window));
  }

  public boolean next() {
    offset += 1;
    if (limitTimestamp < getTimestamp()) return false;
    if (offset < nBlocks) return true;
    return load(blockTsOffset + (nBlocks * window));
  }

  public boolean load(final long blockTsOffset) {
    if (this.blockTsOffset != blockTsOffset) {
      if (!loadData(blockTsOffset)) {
        return false;
      }
    }

    this.blockTsOffset = blockTsOffset;
    this.offset = 0;
    return true;
  }

  protected abstract boolean loadData(final long blockTsOffset);

  @Override
  public int compareTo(final TimeRangeIterator o) {
    return Long.compare(getTimestamp(), o.getTimestamp());
  }

  // We group time data in blocks of 86400 items.
  //  - 1sec window * 24h = 86400 slots (675 KiB)
  //  - the block 'start' timestamp will be: timestamp - (timestamp % (NBLOCKS * window))
  // which means
  //  - 1min window = 60h per block
  //  - 5min window = 300h per block
  //  - 1h window = 3600h/150day per block
  //  - 1msec window = ~86sec per block
  public static long blockTsOffset(final int nblocks, final long window, final long timestamp) {
    return timestamp - (timestamp % (nblocks * window));
  }
}
