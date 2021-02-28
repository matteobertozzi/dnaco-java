/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.dnaco.telemetry;

import tech.dnaco.strings.HumansUtil.HumanLongValueConverter;

public interface TelemetryCollector {
  String getType();
  TelemetryCollectorData getSnapshot();

  final class Builder {
    private HumanLongValueConverter unit;
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
      return register(TelemetryCollectorRegistry.INSTANCE, collector);
    }

    public <T extends TelemetryCollector> T register(final TelemetryCollectorGroup group, final T collector) {
      return group.register(name, label, help, unit, collector);
    }
  }
}
