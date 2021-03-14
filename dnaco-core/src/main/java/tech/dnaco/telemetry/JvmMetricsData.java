package tech.dnaco.telemetry;

import tech.dnaco.strings.HumansUtil;
import tech.dnaco.strings.HumansUtil.HumanLongValueConverter;
import tech.dnaco.util.BuildInfo;
import tech.dnaco.util.Serialization.SerializeWithSnakeCase;

@SerializeWithSnakeCase
public class JvmMetricsData implements TelemetryCollectorData {
  // process
  private final BuildInfo buildInfo;
  private final long uptime;
  private final long pid;
  private final long fds;
  // memory
  private final long totalMemory;
  private final long availMemory;
  private final long freeMemory;
  private final long usedMemory;
  private final long maxMemory;
  // threads
  private final int threadCount;

  public JvmMetricsData(final JvmMetrics metrics) {
    // process
    this.buildInfo = JvmMetrics.INSTANCE.getBuildInfo();
    this.uptime = metrics.getUptime();
    this.pid = metrics.getPid();
    this.fds = metrics.getOpenFileDescriptorCount();
    // memory
    this.totalMemory = metrics.getTotalMemory();
    this.availMemory = metrics.getAvailableMemory();
    this.freeMemory = metrics.getFreeMemory();
    this.usedMemory = metrics.getUsedMemory();
    this.maxMemory = metrics.getMaxMemory();
    // threads
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
	public StringBuilder toHumanReport(final StringBuilder report, final HumanLongValueConverter humanConverter) {
		// Java Version
    report.append(JvmMetrics.INSTANCE.getJavaVersion()).append("\n");

    // Build Info
    if (buildInfo != null) {
      report.append(" - BuildInfo: ");
      report.append(buildInfo.getName()).append(" ").append(buildInfo.getVersion());
      report.append(" (").append(buildInfo.getBuildDate()).append(")\n");
      report.append(" - Built by ").append(buildInfo.getCreatedBy());
      report.append(" from ").append(buildInfo.getGitBranch());
      report.append(" ").append(buildInfo.getGitHash());
      report.append("\n");
    }

    // OS
    report.append(" - OS: ");
    report.append(JvmMetrics.INSTANCE.getOsName()).append(" ");
    report.append(JvmMetrics.INSTANCE.getOsVersion()).append(" (");
    report.append(JvmMetrics.INSTANCE.getOsArch()).append(")\n");

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
    report.append(" - PID: ").append(pid);
    report.append("\n");

    // JVM Threads - TODO: By state
    report.append(" - Threads:");
    report.append(" Active ").append(threadCount);
    report.append("\n");

    // TODO: FDs count
    if (fds > 0) {
      report.append(" - Number of open fd: ").append(fds);
      report.append("\n");
    }

    return report;
	}
}
