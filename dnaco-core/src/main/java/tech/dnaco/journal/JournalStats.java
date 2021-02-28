/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package tech.dnaco.journal;

import java.util.concurrent.TimeUnit;

import tech.dnaco.strings.HumansUtil;
import tech.dnaco.telemetry.Histogram;
import tech.dnaco.telemetry.MaxAndAvgTimeRangeGauge;
import tech.dnaco.telemetry.TelemetryCollector;
import tech.dnaco.telemetry.TelemetryCollectorGroup;

public final class JournalStats extends TelemetryCollectorGroup {
  private final MaxAndAvgTimeRangeGauge bufferUsage = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_SIZE)
    .setName("journal_buffer_usage")
    .setLabel("Journal Buffer Usage")
    .register(this, new MaxAndAvgTimeRangeGauge(24 * 60, 1, TimeUnit.MINUTES));

  private final MaxAndAvgTimeRangeGauge flushTime = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_TIME_NANOS)
    .setName("journal_flush_time")
    .setLabel("Journal Flush Time")
    .register(this, new MaxAndAvgTimeRangeGauge(24 * 60, 1, TimeUnit.MINUTES));

  private final Histogram flushTimeHisto = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_TIME_NANOS)
      .setName("journal_flush_time_histo")
      .setLabel("Journal Flush Time")
      .register(this, new Histogram(Histogram.DEFAULT_DURATION_BOUNDS_NS));

  private final Histogram manageOldLogsTimeHisto = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_TIME_NANOS)
    .setName("journal_manage_old_logs_time_histo")
    .setLabel("Journal Manage Old Logs Time")
    .register(this, new Histogram(Histogram.DEFAULT_DURATION_BOUNDS_NS));

  public void addFlush(final long now, final int tenants, final long bufSize, final long elapsedNs) {
    bufferUsage.set(now, bufSize);
    flushTime.set(now, elapsedNs);
    flushTimeHisto.add(elapsedNs);
  }

  public void addManageOldLogs(final long elapsedNs) {
    manageOldLogsTimeHisto.add(elapsedNs);
  }
}
