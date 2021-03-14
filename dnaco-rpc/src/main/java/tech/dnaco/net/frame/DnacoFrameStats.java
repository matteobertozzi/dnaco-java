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

package tech.dnaco.net.frame;

import java.util.concurrent.TimeUnit;

import tech.dnaco.logging.Logger;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.telemetry.ConcurrentHistogram;
import tech.dnaco.telemetry.ConcurrentMaxAndAvgTimeRangeGauge;
import tech.dnaco.telemetry.ConcurrentTimeRangeCounter;
import tech.dnaco.telemetry.Histogram;
import tech.dnaco.telemetry.TelemetryCollector;
import tech.dnaco.telemetry.TelemetryCollectorGroup;

public class DnacoFrameStats extends TelemetryCollectorGroup {
  private static final String GROUP_NAME = "dnaco_frames";
  private static final String GROUP_LABEL = "DNACO Frames";

  public static final DnacoFrameStats INSTANCE = new TelemetryCollector.Builder()
    .setName(GROUP_NAME)
    .setLabel(GROUP_LABEL)
    .register(new DnacoFrameStats());

  private final ConcurrentTimeRangeCounter frameWriteSize = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_SIZE)
    .setName("frame_write_size")
    .setLabel("Frame Writes size per minutes")
    .register(this, new ConcurrentTimeRangeCounter(24 * 60L, 1L, TimeUnit.MINUTES));

  private final ConcurrentTimeRangeCounter frameWriteMin = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_COUNT)
    .setName("frame_write_count_min")
    .setLabel("Frame Writes per minutes")
    .register(this, new ConcurrentTimeRangeCounter(60L, 1L, TimeUnit.MINUTES));

  private final ConcurrentTimeRangeCounter frameWriteSec = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_COUNT)
    .setName("frame_write_count_sec")
    .setLabel("Frame Writes per seconds")
    .register(this, new ConcurrentTimeRangeCounter(60L, 1L, TimeUnit.SECONDS));

  private final ConcurrentHistogram frameWriteSizeHisto = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_SIZE)
    .setName("frame_write_size_histo")
    .setLabel("Frame Write Size Histogram")
    .register(this, new ConcurrentHistogram(Histogram.DEFAULT_SIZE_BOUNDS));

  private final ConcurrentTimeRangeCounter frameReadSize = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_SIZE)
    .setName("frame_read_size")
    .setLabel("Frame Reads size per minutes")
    .register(this, new ConcurrentTimeRangeCounter(24 * 60L, 1L, TimeUnit.MINUTES));

  private final ConcurrentTimeRangeCounter frameReadMin = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_COUNT)
    .setName("frame_read_count_min")
    .setLabel("Frame Reads per minutes")
    .register(this, new ConcurrentTimeRangeCounter(60L, 1L, TimeUnit.MINUTES));

  private final ConcurrentTimeRangeCounter frameReadSec = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_COUNT)
    .setName("frame_read_count_sec")
    .setLabel("Frame Reads per seconds")
    .register(this, new ConcurrentTimeRangeCounter(60L, 1L, TimeUnit.SECONDS));

  private final ConcurrentHistogram frameReadSizeHisto = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_SIZE)
    .setName("frame_read_size_histo")
    .setLabel("Frame Read Size Histogram")
    .register(this, new ConcurrentHistogram(Histogram.DEFAULT_SIZE_BOUNDS));

  private final ConcurrentMaxAndAvgTimeRangeGauge frameProcessTime = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_TIME_NANOS)
    .setName("frame_process_time")
    .setLabel("Frame Process time")
    .register(this, new ConcurrentMaxAndAvgTimeRangeGauge(60, 1, TimeUnit.MINUTES));

  private final ConcurrentHistogram frameProcessTimeHisto = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_TIME_NANOS)
      .setName("frame_process_time_histo")
      .setLabel("Frame Process time histogram")
      .register(this, new ConcurrentHistogram(Histogram.DEFAULT_DURATION_BOUNDS_NS));

  private DnacoFrameStats() {
    // no-op
  }

  public void addWriteFrame(final int length) {
    this.frameWriteSec.inc();
    this.frameWriteMin.inc();
    this.frameWriteSize.inc(length);
    this.frameWriteSizeHisto.add(length);
  }

  public void addReadFrame(final int length) {
    this.frameReadSec.inc();
    this.frameReadMin.inc();
    this.frameReadSize.inc(length);
    this.frameReadSizeHisto.add(length);
  }

  public void addDecodedFrames(final int frameCount, final long totalSize, final long elapsedNs) {
    if (elapsedNs > 100000000) {
      Logger.trace("decode {} frames in {} total size {}",
        frameCount, HumansUtil.humanTimeNanos(elapsedNs), HumansUtil.humanSize(totalSize));
    }
  }

  public void addProcessTime(final long elapsedNs) {
    this.frameProcessTime.update(elapsedNs);
    this.frameProcessTimeHisto.add(elapsedNs);
  }
}
