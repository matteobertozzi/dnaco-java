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

import java.lang.management.ManagementFactory;

import tech.dnaco.strings.StringUtil;
import tech.dnaco.util.BuildInfo;

public final class JvmMetrics implements TelemetryCollector {
  public static final JvmMetrics INSTANCE = new JvmMetrics();

  private BuildInfo buildInfo = null;

  private JvmMetrics() {
    // no-op
  }


  // ================================================================================
  //  BuildInfo related
  // ================================================================================
  public BuildInfo getBuildInfo() {
    return buildInfo;
  }

  public void setBuildInfo(final BuildInfo buildInfo) {
    this.buildInfo = buildInfo;
  }

  public void setBuildInfoIfNotSet(final BuildInfo buildInfo) {
    if (this.buildInfo == null) {
      this.buildInfo = buildInfo;
    }
  }

  // ================================================================================
  //  Uptime Related
  // ================================================================================
  public long getStartTime() {
    return ManagementFactory.getRuntimeMXBean().getStartTime();
  }

  public long getUptime() {
    return ManagementFactory.getRuntimeMXBean().getUptime();
  }

  // ================================================================================
  //  Memory Related
  // ================================================================================
  public long getUsedMemory() {
    final Runtime runtime = Runtime.getRuntime();
    return runtime.totalMemory() - runtime.freeMemory();
  }

  public long getAvailableMemory() {
    final Runtime runtime = Runtime.getRuntime();
    return runtime.maxMemory() - (runtime.totalMemory() - runtime.freeMemory());
  }

  public long getMaxMemory() {
    return Runtime.getRuntime().maxMemory();
  }

  public long getTotalMemory() {
    return Runtime.getRuntime().totalMemory();
  }

  public long getFreeMemory() {
    return Runtime.getRuntime().freeMemory();
  }

  // ================================================================================
  //  Threads Related
  // ================================================================================
  public int availableProcessors() {
    return Runtime.getRuntime().availableProcessors();
  }

  public int getThreadCount() {
    return Thread.activeCount();
  }

  // ================================================================================
  //  System Related
  // ================================================================================
  public String getJavaVersion() {
    return System.getProperty("java.vm.name")
        + " " + getJavaVersionNumber()
        + " (" + getJavaVendor() + ")";
  }

  public String getJavaVersionNumber() {
    return System.getProperty("java.vm.version");
  }

  public String getJavaVendor() {
    final String vendor = System.getProperty("java.vendor");
    final String vendorVersion = System.getProperty("java.vendor.version");
    if (StringUtil.equals(vendor, vendorVersion)) return vendor;
    return vendor + " " + vendorVersion;
  }

	@Override
	public String getType() {
		return "JVM_METRICS";
	}

	@Override
	public TelemetryCollectorData getSnapshot() {
		return new JvmMetricsData(this);
	}
}