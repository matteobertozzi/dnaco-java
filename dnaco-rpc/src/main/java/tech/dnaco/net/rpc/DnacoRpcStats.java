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

package tech.dnaco.net.rpc;

import java.util.concurrent.TimeUnit;

import io.netty.channel.Channel;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.telemetry.ConcurrentHistogram;
import tech.dnaco.telemetry.ConcurrentTimeRangeCounter;
import tech.dnaco.telemetry.ConcurrentTimeRangeGauge;
import tech.dnaco.telemetry.Histogram;
import tech.dnaco.telemetry.TelemetryCollector;
import tech.dnaco.telemetry.TelemetryCollectorGroup;

public final class DnacoRpcStats extends TelemetryCollectorGroup {
  public static final DnacoRpcStats INSTANCE = new TelemetryCollector.Builder()
    .setName("dnaco_rpc_stats")
    .setLabel("DNACO RPC Stats")
    .register(new DnacoRpcStats());

  private final ConcurrentTimeRangeGauge connectionCount = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_COUNT)
    .setName("connection_count")
    .setLabel("Active connection count")
    .register(this, new ConcurrentTimeRangeGauge(24 * 60L, 1L, TimeUnit.MINUTES));

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

  private void incConnection(final Channel channel) {
    this.connectionCount.inc();
  }

  private void decConnection(final Channel channel) {
    this.connectionCount.dec();
  }

  public void addOutFrame(final int length) {
    this.frameWriteSec.inc();
    this.frameWriteMin.inc();
    this.frameWriteSize.inc(length);
    this.frameWriteSizeHisto.add(length);
  }

  public void addInFrame(final int length) {
    this.frameReadSec.inc();
    this.frameReadMin.inc();
    this.frameReadSize.inc(length);
    this.frameReadSizeHisto.add(length);
  }
}
