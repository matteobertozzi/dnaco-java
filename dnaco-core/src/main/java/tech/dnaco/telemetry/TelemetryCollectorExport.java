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

public class TelemetryCollectorExport {
  public enum TelemetryCollectorType { HISTOGRAM, TIME_RANGE_COUNTER, MAX_AND_AVG_TIME_RANGE_GAUGE, TOP_K, COUNTER_MAP }
  public enum TelemetryCollectorUnit { TIME_NS, TIME_MS, DATE_NS, DATE_MS, COUNT, SIZE }

  private String name;
  private String label;
  private String help;
  private TelemetryCollectorUnit unit;
  private TelemetryCollectorType type;
  private TelemetryCollectorData data;

  public TelemetryCollectorExport() {
    // no-op
  }

  public TelemetryCollectorExport(final String name, final String label, final String help, final String type, final String unit, final TelemetryCollectorData data) {
    System.out.println(name + " -> " + unit);
    this.name = name;
    this.label = label;
    this.help = help;
    this.type = TelemetryCollectorType.valueOf(type);
    this.unit = TelemetryCollectorUnit.valueOf(unit);
    this.data = data;
  }

  public String getName() {
    return name;
  }

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

  public TelemetryCollectorData getData() {
    return data;
  }

  @Override
  public String toString() {
    return "TelemetryCollectorExport [name=" + name + ", type=" + type + ", unit=" + unit +
      ", label=" + label + ", help=" + help + ", data=" + data + "]";
  }
}
