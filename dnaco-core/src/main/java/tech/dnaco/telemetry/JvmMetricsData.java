package tech.dnaco.telemetry;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import tech.dnaco.logging.Logger;
import tech.dnaco.strings.HumansUtil;
import tech.dnaco.strings.HumansUtil.HumanLongValueConverter;
import tech.dnaco.util.BuildInfo;
import tech.dnaco.util.JsonUtil;

public class JvmMetricsData implements TelemetryCollectorData {
  private final long uptime;
  private final long totalMemory;
  private final long availMemory;
  private final long freeMemory;
  private final long usedMemory;
  private final long maxMemory;
  private final int threadCount;

  public JvmMetricsData(final JvmMetrics metrics) {
    this.uptime = metrics.getUptime();
    this.totalMemory = metrics.getTotalMemory();
    this.availMemory = metrics.getAvailableMemory();
    this.freeMemory = metrics.getFreeMemory();
    this.usedMemory = metrics.getUsedMemory();
    this.maxMemory = metrics.getMaxMemory();
    this.threadCount = metrics.getThreadCount();
  }

	public long getUptime() {
		return uptime;
	}

  public long getTotalMemory() {
		return totalMemory;
	}

	public long getAvailMemory() {
		return availMemory;
	}

	public long getFreeMemory() {
		return freeMemory;
	}

	public long getUsedMemory() {
		return usedMemory;
	}

	public long getMaxMemory() {
		return maxMemory;
	}

	public int getThreadCount() {
		return threadCount;
  }


	@Override
	public JsonElement toJson() {
		// Memory
    final JsonObject memJson = new JsonObject();
    memJson.addProperty("total", totalMemory);
    memJson.addProperty("avail", availMemory);
    memJson.addProperty("used", usedMemory);
    memJson.addProperty("free", freeMemory);
    memJson.addProperty("max", maxMemory);

    // Threads
    final JsonObject threadsJson = new JsonObject();
    threadsJson.addProperty("count", threadCount);

    // Jvm Info
    final JsonObject jvmJson = new JsonObject();
    jvmJson.addProperty("name", JvmMetrics.INSTANCE.getJavaVersion());
    jvmJson.addProperty("version", JvmMetrics.INSTANCE.getJavaVersionNumber());
    jvmJson.addProperty("vendor", JvmMetrics.INSTANCE.getJavaVendor());

    // Metrics
    final JsonObject json = new JsonObject();
    json.add("buildInfo", JsonUtil.toJsonTree(JvmMetrics.INSTANCE.getBuildInfo()));
    json.add("jvm", jvmJson);
    json.add("memory", memJson);
    json.add("threads", threadsJson);
    json.addProperty("uptime", uptime);
    return json;
	}

	@Override
	public StringBuilder toHumanReport(StringBuilder report, HumanLongValueConverter humanConverter) {
		// Java Version
    report.append(JvmMetrics.INSTANCE.getJavaVersion()).append("\n");

    // Build Info
    final BuildInfo buildInfo = JvmMetrics.INSTANCE.getBuildInfo();
    if (buildInfo != null) {
      report.append(" - BuildInfo: ");
      report.append(buildInfo.getName()).append(" ").append(buildInfo.getVersion());
      report.append(" (").append(buildInfo.getBuildDate()).append(")\n");
    }

    // JVM Memory
    report.append(" - Memory:");
    report.append(" Max ").append(HumansUtil.humanSize(maxMemory));
    report.append(" - Allocated ").append(HumansUtil.humanSize(totalMemory));
    report.append(" - Used ").append(HumansUtil.humanSize(usedMemory));
    report.append("\n");

    // JVM Uptime
    report.append(" - Uptime: ").append(HumansUtil.humanTimeMillis(uptime));
    report.append("\n");

    // PID
    report.append(" - PID: ").append(ProcessHandle.current().pid());
    report.append("\n");

    // JVM Threads - TODO: By state
    report.append(" - Threads:");
    report.append(" Active ").append(threadCount);
    report.append("\n");

    // TODO: FDs count
    try {
      final OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
      if (os instanceof com.sun.management.UnixOperatingSystemMXBean){
        report.append(" - Number of open fd: ");
        report.append(((com.sun.management.UnixOperatingSystemMXBean) os).getOpenFileDescriptorCount());
        report.append("\n");
      }
    } catch (final Throwable e) {
      Logger.error(e, "unable to fetch number of open fds");
    }

    return report;
	}
}