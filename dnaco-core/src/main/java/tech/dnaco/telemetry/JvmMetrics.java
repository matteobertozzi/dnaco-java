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
import java.lang.management.OperatingSystemMXBean;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.strings.BaseN;
import tech.dnaco.strings.StringUtil;
import tech.dnaco.util.BuildInfo;
import tech.dnaco.util.RandData;

public final class JvmMetrics implements TelemetryCollector {
  public static final JvmMetrics INSTANCE = new JvmMetrics();

  private final ConcurrentMaxAndAvgTimeRangeGauge cpuUsage = new ConcurrentMaxAndAvgTimeRangeGauge(60, 1, TimeUnit.MINUTES);

  private BuildInfo buildInfo = null;

  private JvmMetrics() {
    collect(System.currentTimeMillis());
  }

  public void collect(final long now) {
    cpuUsage.set(now, getCpuUsage());
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
  public long getCpuUsage() {
    final OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
    if (bean instanceof com.sun.management.OperatingSystemMXBean) {
      return Math.round(((com.sun.management.OperatingSystemMXBean)bean).getProcessCpuLoad() * 100);
    }
    return -1;
  }

  protected MaxAndAvgTimeRangeGaugeData getCpuUsageSnapshot() {
    return cpuUsage.getSnapshot();
  }

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

  public String getOsName() {
    return System.getProperty("os.name");
  }

  public String getOsVersion() {
    return System.getProperty("os.version");
  }

  public String getOsArch() {
    return System.getProperty("os.arch");
  }

  public String getDeviceId() {
    try {
      final ArrayList<NetworkInterface> networkIfaces = Collections.list(NetworkInterface.getNetworkInterfaces());
      networkIfaces.sort(Comparator.comparing(NetworkInterface::getName));

      final MessageDigest digest = MessageDigest.getInstance("SHA3-256");
      for (NetworkInterface iface: networkIfaces) {
        final byte[] hwAddr = iface.getHardwareAddress();
        if (BytesUtil.isEmpty(hwAddr)) continue;
        digest.update(iface.getName().getBytes());
        digest.update(iface.getHardwareAddress());
      }
      return "jVm" + BaseN.encodeBase62(digest.digest());
    } catch (SocketException | NoSuchAlgorithmException e) {
      return "jRd-" + BaseN.encodeBase62(RandData.generateBytes(32));
    }
  }

	@Override
	public String getType() {
		return "JVM_METRICS";
	}

	@Override
	public TelemetryCollectorData getSnapshot() {
		return new JvmMetricsData(this);
  }

  public static void main(String[] args) {
    //System.getProperty("os.arch")
    System.out.println(JvmMetrics.INSTANCE.getSnapshot().toHumanReport(new StringBuilder(), null));
  }
}