package tech.dnaco.telemetry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import com.google.gson.JsonObject;

import tech.dnaco.strings.HumansUtil.HumanLongValueConverter;
import tech.dnaco.strings.StringUtil;

public final class TelemetryCollectorRegistry {
  public static final TelemetryCollectorRegistry INSTANCE = new TelemetryCollectorRegistry();

  private final HashMap<String, CollectorInfo> collectorMap = new HashMap<>();

  private TelemetryCollectorRegistry() {
    // no-op
  }

  public void humanReport() {
    final ArrayList<String> keys = new ArrayList<>(collectorMap.keySet());
    Collections.sort(keys);

    final StringBuilder report = new StringBuilder();
    for (String key: keys) {
      final CollectorInfo collectorInfo = collectorMap.get(key);
      report.append("--- ").append(collectorInfo.label).append(" ---\n");
      report.append(collectorInfo.name).append(": ");
      collectorInfo.getSnapshot().toHumanReport(report, collectorInfo.humanConverter);
      report.append('\n');
    }
    System.out.println(report);
  }

  public <T extends TelemetryCollector> T register(final String name, final String label, final String help,
      final HumanLongValueConverter humanConverter, final T collector) {
    final CollectorInfo info = new CollectorInfo(name, label, help, humanConverter, collector);
    collectorMap.put(info.name, info);
    return collector;
  }

  private static final class CollectorInfo {
    private final String name;
    private final String label;
    private final String help;
    private final HumanLongValueConverter humanConverter;
    private final TelemetryCollector collector;

    private CollectorInfo(final String name, final String label, final String help,
        final HumanLongValueConverter humanConverter, final TelemetryCollector collector) {
      this.name = name;
      this.label = label;
      this.help = help;
      this.humanConverter = humanConverter;
      this.collector = collector;
    }

    public JsonObject toJson(final TelemetryCollectorData snapshot) {
      final JsonObject json = new JsonObject();
      json.addProperty("label", label);
      json.addProperty("type", collector.getType());
      json.addProperty("unit", humanConverter.getHumanType());
      json.addProperty("help", help);
      json.add("data", snapshot.toJson());
      return json;
    }

    public TelemetryCollectorData getSnapshot() {
      return collector.getSnapshot();
    }

    @Override
    public int hashCode() {
      return name.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (obj instanceof CollectorInfo) return false;

      final CollectorInfo other = (CollectorInfo) obj;
      return StringUtil.equals(name, other.name);
    }
  }

  public static void main(final String[] args) {
    final Histogram histo = new TelemetryCollector.Builder()
        .setName("")
        .register(new Histogram(Histogram.DEFAULT_DURATION_BOUNDS_MS));
  }
}
