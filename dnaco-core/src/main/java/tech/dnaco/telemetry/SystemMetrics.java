package tech.dnaco.telemetry;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import tech.dnaco.strings.HumansUtil;

public class SystemMetrics {
  public static final SystemMetrics INSTANCE = new SystemMetrics();

  private final ConcurrentMaxAndAvgTimeRangeGauge cpuUsage = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_PERCENT)
    .setName("system.cpu_usage")
    .setLabel("System CPU Usage")
    .register(new ConcurrentMaxAndAvgTimeRangeGauge(60, 1, TimeUnit.MINUTES));

  private final MemoryDataGroup memoryGroup = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_SIZE)
    .setName("system.memory")
    .setLabel("System Memory")
    .register(new MemoryDataGroup());

  private final ConcurrentHashMap<String, DiskDataGroup> diskGroups = new ConcurrentHashMap<>();

  public SystemMetrics() {
    collect(System.currentTimeMillis());
  }

  public void addDiskMonitor(final String name, final File root) {
    final DiskDataGroup group = new DiskDataGroup(name, root);
    TelemetryCollectorRegistry.INSTANCE.register("system.disk_" + name, "Disk " + root.getAbsolutePath(), null, null, group);
    diskGroups.put(name, group);
    collect(System.currentTimeMillis());
  }

  public void collect(final long now) {
    // cpu stats
    cpuUsage.set(now, getCpuUsage());

    // update memory stats
    memoryGroup.update(now);

    // update disk stats
    for (final DiskDataGroup disk: this.diskGroups.values()) {
      disk.update(now);
    }
  }

  public long getCpuUsage() {
    final OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
    if (bean instanceof com.sun.management.OperatingSystemMXBean) {
      return Math.round(((com.sun.management.OperatingSystemMXBean)bean).getSystemCpuLoad() * 100);
    }
    return -1;
  }

  private static final class MemoryDataGroup extends TelemetryCollectorGroup {
	  private final ConcurrentMaxAndAvgTimeRangeGauge memUsage = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_SIZE)
      .setName("system_mem_usage")
      .setLabel("System Mem Usage")
      .register(this, new ConcurrentMaxAndAvgTimeRangeGauge(60, 1, TimeUnit.MINUTES));

    private final ConcurrentMaxAndAvgTimeRangeGauge memAvail = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_SIZE)
      .setName("system_mem_avail")
      .setLabel("System Mem avail")
      .register(this, new ConcurrentMaxAndAvgTimeRangeGauge(60, 1, TimeUnit.MINUTES));

    private final CounterMap memSpace = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_SIZE)
      .setName("system_mem_space")
      .setLabel("System Mem Space")
      .register(this, new CounterMap(2));

    private final ConcurrentMaxAndAvgTimeRangeGauge swapUsage = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_SIZE)
      .setName("system_swap_usage")
      .setLabel("System Swap Usage")
      .register(this, new ConcurrentMaxAndAvgTimeRangeGauge(60, 1, TimeUnit.MINUTES));

    private final CounterMap swapSpace = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_SIZE)
      .setName("system_swap_space")
      .setLabel("System Swap Space")
      .register(this, new CounterMap(2));

    public void update(final long now) {
      final LinuxMemInfo memInfo = LinuxMemInfo.readProcMemInfo();
      if (memInfo != null) {
        memUsage.set(now, memInfo.getMemUsed());
        memAvail.set(now, memInfo.getMemFree());
        memSpace.set("Used", memInfo.getMemUsed());
        memSpace.set("Avail", memInfo.getMemFree());

        // update swap
        swapUsage.set(now, memInfo.getSwapUsed());
        swapSpace.set("Used", memInfo.getSwapUsed());
        swapSpace.set("Avail", memInfo.getSwapFree());
      } else {
        final long memTotal = getTotalMemory();
        final long memFree = getFreeMemory();
        final long memUsed = memTotal - memFree;
        memUsage.set(now, memUsed);
        memAvail.set(now, memFree);
        memSpace.set("Used", memUsed);
        memSpace.set("Avail", memFree);

        // update swap
        final long swapTotal = getTotalSwap();
        final long swapFree = getFreeSwap();
        final long swapUsed = swapTotal - swapFree;
        swapUsage.set(now, swapUsed);
        swapSpace.set("Used", swapUsed);
        swapSpace.set("Avail", swapFree);
      }
    }

    private long getTotalMemory() {
      final OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
      if (bean instanceof com.sun.management.OperatingSystemMXBean) {
        return ((com.sun.management.OperatingSystemMXBean)bean).getTotalPhysicalMemorySize();
      }
      return -1;
    }

    private long getFreeMemory() {
      final OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
      if (bean instanceof com.sun.management.OperatingSystemMXBean) {
        return ((com.sun.management.OperatingSystemMXBean)bean).getFreePhysicalMemorySize();
      }
      return -1;
    }

    private long getTotalSwap() {
      final OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
      if (bean instanceof com.sun.management.OperatingSystemMXBean) {
        return ((com.sun.management.OperatingSystemMXBean)bean).getTotalSwapSpaceSize();
      }
      return -1;
    }

    private long getFreeSwap() {
      final OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
      if (bean instanceof com.sun.management.OperatingSystemMXBean) {
        return ((com.sun.management.OperatingSystemMXBean)bean).getFreeSwapSpaceSize();
      }
      return -1;
    }
  }

  private static final class DiskDataGroup extends TelemetryCollectorGroup {
    private final ConcurrentMaxAndAvgTimeRangeGauge diskUsage;
    private final ConcurrentMaxAndAvgTimeRangeGauge diskAvail;
    private final CounterMap diskSpace;
    private final File root;

    public DiskDataGroup(final String name, final File root) {
      this.root = root;

      this.diskUsage = new TelemetryCollector.Builder()
        .setUnit(HumansUtil.HUMAN_SIZE)
        .setName("disk_usage")
        .setLabel("Disk Usage " + root)
        .register(this, new ConcurrentMaxAndAvgTimeRangeGauge(60, 1, TimeUnit.MINUTES));
      this.diskAvail = new TelemetryCollector.Builder()
        .setUnit(HumansUtil.HUMAN_SIZE)
        .setName("disk_avail")
        .setLabel("Disk avail " + root)
        .register(this, new ConcurrentMaxAndAvgTimeRangeGauge(60, 1, TimeUnit.MINUTES));
      this.diskSpace = new TelemetryCollector.Builder()
        .setUnit(HumansUtil.HUMAN_SIZE)
        .setName("disk_space")
        .setLabel("Disk space " + root)
        .register(this, new CounterMap(2));
    }

    protected void update(final long now) {
      final long total = root.getTotalSpace();
      final long avail = root.getUsableSpace();
      diskUsage.set(now, total - avail);
      diskAvail.set(now, avail);
      diskSpace.set("Avail", avail);
      diskSpace.set("Used", total - avail);
    }
  }
}
