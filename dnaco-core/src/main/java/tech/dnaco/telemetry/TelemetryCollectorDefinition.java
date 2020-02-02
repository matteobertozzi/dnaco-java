package tech.dnaco.telemetry;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import tech.dnaco.util.JsonUtil;

public class TelemetryCollectorDefinition {
  public enum TelemetryCollectorType { HISTOGRAM, TIME_RANGE_COUNTER, MAX_AND_AVG_TIME_RANGE_GAUGE, TOP_K }
  public enum TelemetryCollectorUnit { TIME_NS, TIME_MS, DATE_NS, DATE_MS, COUNT, SIZE }

  private String label;
  private String help;
  private TelemetryCollectorUnit unit;
  private TelemetryCollectorType type;
  private JsonElement data;

  public String getLabel() {
    return label;
  }

  public String getHelp() {
    return help;
  }

  public TelemetryCollectorUnit getUnit() {
    return unit;
  }

  public TelemetryCollectorType getType() {
    return type;
  }

  public <T> T getData(final Class<T> classOfT) {
    return JsonUtil.fromJson(data, classOfT);
  }

  public JsonObject toJson() {
    final JsonObject json = new JsonObject();
    json.addProperty("label", label);
    json.addProperty("type", unit.name());
    json.addProperty("unit", unit.name());
    json.addProperty("help", help);
    json.add("data", data);
    return json;
  }
}
