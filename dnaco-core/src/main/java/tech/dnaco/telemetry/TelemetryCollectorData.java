package tech.dnaco.telemetry;

import com.google.gson.JsonElement;

import tech.dnaco.strings.HumansUtil.HumanLongValueConverter;

public interface TelemetryCollectorData {
  JsonElement toJson();
  StringBuilder toHumanReport(StringBuilder report, HumanLongValueConverter humanConverter);
}
