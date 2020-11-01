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
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class CounterMap implements TelemetryCollector {
  private final ConcurrentHashMap<String, LongAdder> counters;

  public CounterMap() {
    this.counters = new ConcurrentHashMap<>();
  }

  public CounterMap(final int size) {
    this.counters = new ConcurrentHashMap<>(size);
  }

  public void inc(final String key) {
    inc(key, 1);
  }

  public void inc(final String key, final long amount) {
    LongAdder adder = counters.get(key);
    if (adder == null) {
      adder = counters.computeIfAbsent(key, k -> new LongAdder());
    }
    adder.add(amount);
  }

  public void set(final String key, final long value) {
    final LongAdder adder = new LongAdder();
    adder.add(value);
    counters.put(key, adder);
  }

  public long get(final String key) {
    final LongAdder adder = counters.get(key);
    return adder != null ? adder.sum() : 0;
  }

  public void clear(final String key) {
    final LongAdder adder = counters.get(key);
    if (adder != null) adder.reset();
  }

  public void remove(final String key) {
    counters.remove(key);
  }

  public void clear() {
    counters.clear();
  }

  public Set<String> getKeys() {
    return counters.keySet();
  }

  public int size() {
    return counters.size();
  }

  public boolean isEmpty() {
    return counters.isEmpty();
  }

  public boolean isNotEmpty() {
    return !counters.isEmpty();
  }

  @Override
  public String getType() {
    return "COUNTER_MAP";
  }

  @Override
  public TelemetryCollectorData getSnapshot() {
    final CounterEntry[] entries = new CounterEntry[counters.size()];
    final Iterator<Entry<String, LongAdder>> it = counters.entrySet().iterator();
    for (int i = 0; i < entries.length; ++i) {
      final Entry<String, LongAdder> entry = it.next();
      entries[i] = new CounterEntry(entry.getKey(), entry.getValue().sum());
    }

    Arrays.sort(entries);
    final String[] keys = new String[entries.length];
    final long[] events = new long[entries.length];
    for (int i = 0; i < entries.length; ++i) {
      keys[i] = entries[i].key;
      events[i] = entries[i].event;
    }
    return new CounterMapData(keys, events);
  }

  private static final class CounterEntry implements Comparable<CounterEntry> {
    private final String key;
    private final long event;

    private CounterEntry(final String key, final long event) {
      this.key = key;
      this.event = event;
    }

    @Override
    public int compareTo(final CounterEntry other) {
      final int cmp = Long.compare(other.event, event);
      return cmp != 0 ? cmp : key.compareTo(other.key);
    }
  }
}
