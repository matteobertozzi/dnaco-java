package tech.dnaco.telemetry;

import tech.dnaco.strings.HumansUtil;
import tech.dnaco.strings.HumansUtil.HumanLongValueConverter;

public interface TelemetryCollector {
  String getType();
  TelemetryCollectorData getSnapshot();

  final class Builder {
    private HumanLongValueConverter unit = HumansUtil.HUMAN_COUNT;
    private String name;
    private String label;
    private String help;

    public Builder setName(final String name) {
      this.name = name;
      return this;
    }

    public Builder setLabel(final String label) {
      this.label = label;
      return this;
    }

    public Builder setHelp(final String help) {
      this.help = help;
      return this;
    }

    public Builder setUnit(final HumanLongValueConverter humanConverter) {
      this.unit = humanConverter;
      return this;
    }

    public <T extends TelemetryCollector> T register(final T collector) {
      return TelemetryCollectorRegistry.INSTANCE.register(name, label, help, unit, collector);
    }
  }
}
