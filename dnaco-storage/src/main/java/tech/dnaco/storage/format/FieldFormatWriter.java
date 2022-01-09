/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package tech.dnaco.storage.format;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.data.json.JsonArray;
import tech.dnaco.data.json.JsonObject;
import tech.dnaco.geo.LatLong;

public interface FieldFormatWriter extends Closeable {
  int getVersion();

  void reset();
  void close();

  // Primitive Types
  void writeNull(int fieldId);
  void writeBool(int fieldId, boolean value);
  void writeInt(int fieldId, long value);
  void writeFloat(int fieldId, double value);
  void writeBytes(int fieldId, byte[] value, int off, int len);
  void writeArray(int fieldId, JsonArray array);
  void writeObject(int fieldId, JsonObject object);

  // Geo Types
  void writeGeoLocation(int fieldId, double latitude, double longitude);

  // Helpers
  default void writeString(final int fieldId, final String value) {
    writeBytes(fieldId, value.getBytes(StandardCharsets.UTF_8));
  }

  default void writeBytes(final int fieldId, final byte[] value) {
    writeBytes(fieldId, value, 0, value.length);
  }

  default void writeBytes(final int fieldId, final ByteArraySlice value) {
    writeBytes(fieldId, value.rawBuffer(), value.offset(), value.length());
  }

  default void writeGeoLocation(final int fieldId, final LatLong latlng) {
    writeGeoLocation(fieldId, latlng.getLatitude(), latlng.getLongitude());
  }
}
