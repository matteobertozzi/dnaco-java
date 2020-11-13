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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.concurrent.TimeUnit;

import tech.dnaco.strings.HumansUtil;

public final class JvmGcMetrics extends TelemetryCollectorGroup {
  public static final JvmGcMetrics INSTANCE = new JvmGcMetrics();

  private final ConcurrentMaxAndAvgTimeRangeGauge memUsage = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_SIZE)
    .setName("jvm_mem_usage")
    .setLabel("JVM Mem Usage")
    .register(this, new ConcurrentMaxAndAvgTimeRangeGauge(24 * 60, 5, TimeUnit.MINUTES));

  private final ConcurrentMaxAndAvgTimeRangeGauge fdsUsage = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_COUNT)
    .setName("jvm_fds")
    .setLabel("JVM Open FDs")
    .register(this, new ConcurrentMaxAndAvgTimeRangeGauge(60, 1, TimeUnit.MINUTES));

  private static final class GcDataGroup extends TelemetryCollectorGroup {
    private final TimeRangeCounter collectionCount = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_COUNT)
      .setName("collection_count")
      .setLabel("Collection Count")
      .register(this, new TimeRangeCounter(24 * 60, 5, TimeUnit.MINUTES));

    private final TimeRangeCounter collectionTime = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_TIME_MILLIS)
      .setName("collection_time")
      .setLabel("Collection Time")
      .register(this, new TimeRangeCounter(24 * 60, 5, TimeUnit.MINUTES));

    private long totalCollectionCount;
    private long totalCollectionTime;

    private synchronized void update(final long now, final GarbageCollectorMXBean gc) {
      final long count = gc.getCollectionCount();
      final long time = gc.getCollectionTime();
      collectionCount.add(now, count - this.totalCollectionCount);
      collectionTime.add(now, time - this.totalCollectionTime);
      this.totalCollectionCount = count;
      this.totalCollectionTime = time;
    }
  }

  private JvmGcMetrics() {
    collect(System.currentTimeMillis());
  }

  public long getOpenFds() {
    final OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
    if (bean instanceof com.sun.management.UnixOperatingSystemMXBean){
      return ((com.sun.management.UnixOperatingSystemMXBean) bean).getOpenFileDescriptorCount();
    }
    return -1;
  }

  public void collect(final long now) {
    // update fds stats
    fdsUsage.set(now, getOpenFds());

    // update memory stats
    memUsage.set(now, JvmMetrics.INSTANCE.getUsedMemory());

    // update garbage collector stats
    for (final GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
      final String name = gc.getName().toLowerCase().replaceAll(" ", "_");
      final GcDataGroup group = this.get(name);
      if (group != null) {
        group.update(now, gc);
        continue;
      }

      final GcDataGroup gcMetrics = this.register(name, gc.getName(), null, new GcDataGroup());
      gcMetrics.update(now, gc);
    }
  }
}