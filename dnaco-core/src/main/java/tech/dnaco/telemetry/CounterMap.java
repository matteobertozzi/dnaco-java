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

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class CounterMap implements TelemetryCollector {
  private final ConcurrentHashMap<String, LongAdder> counters = new ConcurrentHashMap<>();

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
    LongAdder adder = counters.get(key);
    if (adder == null) {
      adder = counters.computeIfAbsent(key, k -> new LongAdder());
    }
    adder.reset();
    adder.add(value);
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
    final String[] keys = new String[counters.size()];
    final long[] events = new long[keys.length];
    final Iterator<Entry<String, LongAdder>> it = counters.entrySet().iterator();
    for (int i = 0; i < keys.length; ++i) {
      final Entry<String, LongAdder> entry = it.next();
      keys[i] = entry.getKey();
      events[i] = entry.getValue().sum();
    }
    return new CounterMapData(keys, events);
  }
}
