package tech.dnaco.telemetry;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import tech.dnaco.strings.HumansUtil;

public final class SystemMetrics extends TelemetryCollectorGroup {
  public static final SystemMetrics INSTANCE = new SystemMetrics();

  private final ConcurrentHashMap<String, DiskDataGroup> diskGroups = new ConcurrentHashMap<>();

  private final ConcurrentMaxAndAvgTimeRangeGauge cpuUsage = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_PERCENT)
      .setName("system_cpu_usage")
      .setLabel("System CPU Usage")
      .register(this, new ConcurrentMaxAndAvgTimeRangeGauge(60, 1, TimeUnit.MINUTES));

  private final MemoryDataGroup memoryGroup = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_SIZE)
    .setName("system_memory")
    .setLabel("System Memory")
    .register(this, new MemoryDataGroup());

  public SystemMetrics() {
    collect(System.currentTimeMillis());
  }

  public void addDiskMonitor(final String name, final File root) {
    final DiskDataGroup group = new DiskDataGroup(name, root);
    register("disk_" + name, "Disk " + root.getAbsolutePath(), null, null, group);
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

  // ================================================================================
  //  Threads Related
  // ================================================================================
  public long getCpuUsage() {
    final OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
    if (bean instanceof com.sun.management.OperatingSystemMXBean) {
      return Math.round(((com.sun.management.OperatingSystemMXBean)bean).getSystemCpuLoad() * 100);
    }
    return -1;
  }

  /*
  private long lastCpuSum = 0;
  private long lastCpuIdle = 0;
  public long getCpuUsage() {
    final List<String> stats;
    try {
      stats = Files.readAllLines(Paths.get("/proc/stat"));
    } catch (IOException e) {
      return 0;
    }

    final String[] cpuNow = stats.get(0).split("\\s+");
    long cpuSum = 0;
    for (int i = 1; i < cpuNow.length; ++i) cpuSum += StringConverter.toLong(cpuNow[i], 0);

    final long cpuNowIdle = StringConverter.toLong(cpuNow[4], 0);
    final long cpuDelta = cpuSum - lastCpuSum;
    final long cpuIdle = cpuNowIdle - lastCpuIdle;

    final long cpuUsed = cpuDelta - cpuIdle;
    final long cpuUsage = (100 * cpuUsed / cpuDelta);

    lastCpuSum = cpuSum;
    lastCpuIdle = cpuNowIdle;

    return cpuUsage;
  }
  */

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

    public void update(final long now) {
      final long memUsed = getUsedMemory();
      final long memFree = getFreeMemory();
      memUsage.set(now, memUsed);
      memAvail.set(now, memFree);
      memSpace.set("Used", memUsed);
      memSpace.set("Avail", memFree);
    }

    public long getTotalMemory() {
      final OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
      if (bean instanceof com.sun.management.OperatingSystemMXBean) {
        return ((com.sun.management.OperatingSystemMXBean)bean).getTotalPhysicalMemorySize();
      }
      return -1;
    }

    public long getFreeMemory() {
      final OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
      if (bean instanceof com.sun.management.OperatingSystemMXBean) {
        return ((com.sun.management.OperatingSystemMXBean)bean).getFreePhysicalMemorySize();
      }
      return -1;
    }

    public long getUsedMemory() {
      return getTotalMemory() - getFreeMemory();
    }
  }

  private static final class DiskDataGroup extends TelemetryCollectorGroup {
    private final ConcurrentMaxAndAvgTimeRangeGauge diskUsage = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_SIZE)
      .setName("disk_usage")
      .setLabel("Disk Usage")
      .register(this, new ConcurrentMaxAndAvgTimeRangeGauge(60, 1, TimeUnit.MINUTES));

    private final ConcurrentMaxAndAvgTimeRangeGauge diskAvail = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_SIZE)
      .setName("disk_avail")
      .setLabel("Disk avail")
      .register(this, new ConcurrentMaxAndAvgTimeRangeGauge(60, 1, TimeUnit.MINUTES));

    private final CounterMap diskSpace = new TelemetryCollector.Builder()
      .setUnit(HumansUtil.HUMAN_SIZE)
      .setName("disk_space")
      .setLabel("Disk space")
      .register(this, new CounterMap(2));

    private final File root;

    public DiskDataGroup(final String name, final File root) {
      this.root = root;
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