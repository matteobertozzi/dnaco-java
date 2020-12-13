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
    json.addProperty("type", type.name());
    json.addProperty("unit", unit.name());
    json.addProperty("help", help);
    json.add("data", data);
    return json;
  }
}
