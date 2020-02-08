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

import java.util.Arrays;
import java.util.Comparator;

import tech.dnaco.strings.HumansUtil;
import tech.dnaco.strings.StringUtil;
import tech.dnaco.telemetry.TopKData.TopEntry;
import tech.dnaco.util.BitUtil;

public class TopK implements TelemetryCollector {
  public enum TopType { COUNT, MIN_MAX }

  private final MinMaxEntry[] entries;
  private final int[] buckets;
  private final TopType type;
  private final int k;

  private int entryCount = 0;

  public TopK(final TopType type, final int k) {
    this.k = k;
    this.type = type;
    this.buckets = new int[BitUtil.nextPow2(k)];
    this.entries = new MinMaxEntry[k * 2];
    Arrays.fill(buckets, -1);
  }

  public void add(final String key, final long value) {
    add(key, value, null);
  }

  public synchronized void add(final String key, final long value, final String traceId) {
    final int keyHash = hash(key);
    final int entryIndex = findEntry(key, keyHash);
    final MinMaxEntry entry;
    if (entryIndex >= 0) {
      entry = entries[entryIndex];
    } else {
      entry = addNewEntry(key, keyHash);
    }
    entry.add(value, traceId);
  }

  private int findEntry(final String key, final int keyHash) {
    int index = buckets[keyHash & (buckets.length - 1)];
    while (index >= 0) {
      final MinMaxEntry entry = entries[index];
      if (entry.keyHash == keyHash && StringUtil.equals(entry.key, key)) {
        return index;
      }
      index = entry.next;
    }
    return -1;
  }

  private MinMaxEntry addNewEntry(final String key, final int keyHash) {
    if (entryCount == entries.length) sortAndRebuild(k + (k / 2));

    final MinMaxEntry entry = new MinMaxEntry(key, keyHash);
    final int targetBucket = keyHash & (buckets.length - 1);
    entry.next = buckets[targetBucket];
    buckets[targetBucket] = entryCount;
    entries[entryCount++] = entry;
    return entry;
  }

  private void sortAndRebuild(final int toKeepCount) {
    Arrays.sort(entries, 0, entryCount, comparatorByType());

    this.entryCount = toKeepCount;
    for (int i = toKeepCount, n = entries.length; i < n; ++i) {
      entries[i] = null;
    }

    Arrays.fill(buckets, -1);
    for (int i = 0, n = entryCount; i < n; ++i) {
      final MinMaxEntry entry = entries[i];
      final int targetBucket = entry.keyHash & (buckets.length - 1);
      entry.next = buckets[targetBucket];
      buckets[targetBucket] = i;
    }
  }

  private static int hash(final String key) {
    int h = key.hashCode() & 0x7fffffff;
    h = ((h >>> 16) ^ h) * 0x45d9f3b;
    h = ((h >>> 16) ^ h) * 0x45d9f3b;
    h = (h >>> 16) ^ h;
    return h & 0x7fffffff;
  }

  @Override
  public String getType() {
    return "TOP_K";
  }

  @Override
  public TopKData getSnapshot() {
    if (entryCount == 0) return TopKData.EMPTY;

    Arrays.sort(entries, 0, entryCount, comparatorByType());

    final TopEntry[] topEntries = new TopEntry[Math.min(entryCount, k)];
    for (int i = 0; i < topEntries.length; ++i) {
      final MinMaxEntry entry = entries[i];

      final String[] traceIds;
      if (entry.traceIdIndex > 0) {
        traceIds = new String[(int) Math.min(entry.traceIds.length, entry.traceIdIndex)];
        for (int k = 0; k < traceIds.length; ++k) {
          traceIds[k] = entry.traceIds[(int)((entry.traceIdIndex - (k + 1)) % traceIds.length)];
        }
      } else {
        traceIds = null;
      }

      topEntries[i] = new TopEntry(entry.key, entry.maxTs, entry.vMax, entry.vMin,
        entry.vSum, entry.freq, traceIds);
    }
    return new TopKData(topEntries);
  }

  private Comparator<MinMaxEntry> comparatorByType() {
    switch (type) {
      case COUNT: return SORT_BY_FREQ;
      case MIN_MAX: return SORT_BY_MAX_VALUE;
    }
    throw new UnsupportedOperationException(type.name());
  }

  private static final Comparator<MinMaxEntry> SORT_BY_FREQ = new Comparator<>() {
    @Override
    public int compare(final MinMaxEntry a, final MinMaxEntry b) {
      return Long.compare(b.freq, a.freq);
    }
  };

  private static final Comparator<MinMaxEntry> SORT_BY_MAX_VALUE = new Comparator<>() {
    @Override
    public int compare(final MinMaxEntry a, final MinMaxEntry b) {
      return Long.compare(b.vMax, a.vMax);
    }
  };

  private static final class MinMaxEntry {
    private final String key;
    private final int keyHash;
    private int next;

    private long maxTs = 0;
    private long vMax = Long.MIN_VALUE;
    private long vMin = Long.MAX_VALUE;
    private long vSum = 0;
    private long freq = 0;

    private final String[] traceIds = new String[4];
    private long traceIdIndex = 0;

    private MinMaxEntry(final String key, final int keyHash) {
      this.key = key;
      this.keyHash = keyHash;
      this.next = -1;
    }

    private void add(final long value, final String traceId) {
      if (value >= vMax) {
        vMax = value;
        maxTs = System.currentTimeMillis();
        if (StringUtil.isNotEmpty(traceId)) {
          traceIds[(int)(traceIdIndex++ % traceIds.length)] = traceId;
        }
      }
      vMin = Math.min(vMin, value);
      vSum += value;
      freq++;
    }
  }

  public static void main(String[] args) throws Exception {
    final long startTime = System.nanoTime();
    final TopK topK  = new TopK(TopType.MIN_MAX, 10);
    for (int i = 0; i < 1_000_000; ++i) {
      topK.add("foo-" + (i % 6), i);
    }
    System.out.println(HumansUtil.humanTimeSince(startTime));
    System.out.println(topK.getSnapshot().toHumanReport(new StringBuilder(), HumansUtil.HUMAN_COUNT));
  }
}