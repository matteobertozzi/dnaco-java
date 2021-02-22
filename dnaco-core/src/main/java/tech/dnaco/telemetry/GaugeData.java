package tech.dnaco.telemetry;

import tech.dnaco.data.JsonFormatUtil.JsonElement;
import tech.dnaco.data.JsonFormatUtil.JsonPrimitive;
import tech.dnaco.strings.HumansUtil.HumanLongValueConverter;

public class GaugeData implements TelemetryCollectorData {
  private final long value;

  public GaugeData(final long value) {
    this.value = value;
  }

  @Override
  public JsonElement toJson() {
    return new JsonPrimitive(value);
  }

  @Override
  public StringBuilder toHumanReport(final StringBuilder report, final HumanLongValueConverter humanConverter) {
    return report.append(humanConverter.toHuman(value));
  }
}
