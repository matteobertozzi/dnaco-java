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
import java.util.Set;
import java.util.concurrent.TimeUnit;

import tech.dnaco.strings.HumansUtil;
import tech.dnaco.telemetry.JvmThreadsMetricsData.JvmThreadInfo;

public final class JvmThreadsMetrics implements TelemetryCollector {
  public static final JvmThreadsMetrics INSTANCE = new JvmThreadsMetrics();

  private final MaxAndAvgTimeRangeGauge threadCount = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_COUNT)
    .setName("jvm.threads.thread_count")
    .setLabel("JVM Thread count")
    .register(new MaxAndAvgTimeRangeGauge(60, 1, TimeUnit.MINUTES));

  private JvmThreadsMetrics() {
    // no-op
  }

	@Override
	public String getType() {
		return "JVM_THREADS";
	}

  public void collect(final long now) {
    threadCount.set(now, Thread.activeCount());
  }

	@Override
	public JvmThreadsMetricsData getSnapshot() {
    final Set<Thread> threads = Thread.getAllStackTraces().keySet();
    final JvmThreadInfo[] threadInfo = new JvmThreadInfo[threads.size()];
    int index = 0;
    for (final Thread t : threads) {
      threadInfo[index++] = new JvmThreadInfo(t);
    }
    Arrays.sort(threadInfo);
    return new JvmThreadsMetricsData(threadInfo);
	}
}
