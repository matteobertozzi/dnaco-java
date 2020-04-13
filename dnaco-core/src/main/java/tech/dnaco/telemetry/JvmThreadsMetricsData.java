package tech.dnaco.telemetry;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

import tech.dnaco.strings.HumansTableView;
import tech.dnaco.strings.HumansUtil.HumanLongValueConverter;
import tech.dnaco.strings.StringUtil;
import tech.dnaco.util.JsonUtil;

public class JvmThreadsMetricsData implements TelemetryCollectorData {
  private final JvmThreadInfo[] threadInfo;

	public JvmThreadsMetricsData(JvmThreadInfo[] threadInfo) {
    this.threadInfo = threadInfo;
	}

	@Override
	public JsonElement toJson() {
		final JsonArray jarray = new JsonArray();
    for (int i = 0; i < threadInfo.length; ++i) {
      jarray.add(JsonUtil.toJsonTree(threadInfo[i]));
    }
    return jarray;
	}

	@Override
	public StringBuilder toHumanReport(StringBuilder report, HumanLongValueConverter humanConverter) {
		final HumansTableView table = new HumansTableView();
    table.addColumns("state", "name", "group", "type", "priority");
    for (int i = 0; i < threadInfo.length; ++i) {
      final JvmThreadInfo t = threadInfo[i];
      table.addRow(t.getState(), t.getName(), t.getGroup(), t.isDaemon() ? "Daemon" : "Normal", t.getPriority());
    }
    report.append("count ").append(threadInfo.length).append("\n");
    return table.addHumanView(report);
  }

  public static final class JvmThreadInfo implements Comparable<JvmThreadInfo> {
    private final String name;
    private final String group;
    private final Thread.State state;
    private final boolean daemon;
    private final int priority;

    public JvmThreadInfo(final Thread thread) {
      final ThreadGroup group = thread.getThreadGroup();
      this.name = thread.getName();
      this.state = thread.getState();
      this.priority = thread.getPriority();
      this.daemon = thread.isDaemon();
      this.group = group != null ? group.getName() : null;
    }

    public String getName() {
      return name;
    }

    public String getGroup() {
      return StringUtil.isEmpty(group) ? "" : group;
    }

    public Thread.State getState() {
      return state;
    }

    public boolean isDaemon() {
      return daemon;
    }

    public int getPriority() {
      return priority;
    }

    @Override
    public int compareTo(final JvmThreadInfo other) {
      final int cmp = state.compareTo(other.state);
      return (cmp != 0) ? cmp : name.compareTo(other.name);
    }
  }
}