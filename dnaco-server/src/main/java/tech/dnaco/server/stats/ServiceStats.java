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

package tech.dnaco.server.stats;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

import tech.dnaco.strings.HumansUtil;
import tech.dnaco.telemetry.ConcurrentHistogram;
import tech.dnaco.telemetry.TelemetryCollector;
import tech.dnaco.telemetry.TelemetryCollectorGroup;
import tech.dnaco.telemetry.TimeRangeGauge;

public class ServiceStats extends TelemetryCollectorGroup {
  private final TimeRangeGauge queueHourly = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_COUNT)
      .setName("queue_hourly")
      .setLabel("Service Queue Length hourly")
      .register(this, new TimeRangeGauge(60, 1, TimeUnit.MINUTES));

  private final ConcurrentHistogram queueSizeHisto = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_COUNT)
      .setName("server_queue_histo")
      .setLabel("Queue Size histogram")
      .register(this, new ConcurrentHistogram(1, 2, 4, 8, 16, 32, 48, 64, 128, 256, 512, 1024));

	public ServiceStats(final String serviceName, final int port) {
    this(serviceName + '_' + port);
  }

  public ServiceStats(final String name) {
		super(name);
	}

  public long addRequestToQueue(final long readTime) {
    final long queueSize = queueHourly.inc();
    queueSizeHisto.add(queueSize - 1);
    return queueSize;
  }

  public long removeRequestFromQueue() {
    return queueHourly.dec();
  }

  public void addChannel(SocketAddress remoteAddress) {
  }

  public void removeChannel() {
  }
}
