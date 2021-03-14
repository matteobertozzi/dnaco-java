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

package tech.dnaco.data.json;

import java.util.Objects;

import tech.dnaco.strings.StringConverter;

public class JsonPrimitive extends JsonElement {
  private final Object value;

  public JsonPrimitive(final boolean value) {
    this.value = value;
  }

  public JsonPrimitive(final long value) {
    this.value = value;
  }

  public JsonPrimitive(final double value) {
    this.value = value;
  }

  public JsonPrimitive(final String value) {
    this.value = value;
  }

  public JsonPrimitive(final Number value) {
    this.value = value;
  }

  protected Object getValue() {
    return value;
  }

  public boolean isBoolean() {
    return value instanceof Boolean;
  }

  @Override
  public boolean getAsBoolean() {
    if (isBoolean()) {
      return ((Boolean) value).booleanValue();
    }
    return StringConverter.toBoolean(getAsString(), false);
  }

  public boolean isNumber() {
    return value instanceof Number;
  }

  @Override
  public Number getAsNumber() {
    return (Number) value;
  }

  public boolean isString() {
    return value instanceof String;
  }

  @Override
  public String getAsString() {
    return String.valueOf(value);
  }

  @Override
  public double getAsDouble() {
    return isNumber() ? getAsNumber().doubleValue() : Double.parseDouble(getAsString());
  }

  @Override
  public float getAsFloat() {
    return isNumber() ? getAsNumber().floatValue() : Float.parseFloat(getAsString());
  }

  @Override
  public long getAsLong() {
    return isNumber() ? getAsNumber().longValue() : Long.parseLong(getAsString());
  }

  @Override
  public short getAsShort() {
    return isNumber() ? getAsNumber().shortValue() : Short.parseShort(getAsString());
  }

  @Override
  public int getAsInt() {
    return isNumber() ? getAsNumber().intValue() : Integer.parseInt(getAsString());
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }

  @Override
  public boolean equals(final Object o) {
    return (o == this) || (o instanceof JsonPrimitive && Objects.equals(((JsonPrimitive) o).value, value));
  }
}
