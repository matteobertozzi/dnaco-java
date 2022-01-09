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

import java.nio.charset.StandardCharsets;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.data.json.JsonArray;
import tech.dnaco.data.json.JsonObject;
import tech.dnaco.geo.LatLong;
import tech.dnaco.storage.DataTypes.DataType;

public interface FieldFormatReader {
  int getVersion();

  void load(ByteArraySlice block);

  // Primitive Types
  boolean isNull(int fieldId);
  boolean getBool(int fieldId);
  long getInt(int fieldId);
  double getFloat(int fieldId);
  ByteArraySlice getBytes(int fieldId);
  JsonArray getArray(int fieldId);
  JsonObject getObject(int fieldId);

  // Geo Types
  LatLong getGeoLocation(int fieldId);

  // Helpers
  default String getString(final int fieldId) {
    final ByteArraySlice bytes = getBytes(fieldId);
    return new String(bytes.rawBuffer(), bytes.offset(), bytes.length(), StandardCharsets.UTF_8);
  }

  default Object get(final DataType type, final int fieldId) {
    if (isNull(fieldId)) return null;

    return switch (type) {
      case BOOL -> getBool(fieldId);
      case INT -> getInt(fieldId);
      case FLOAT -> getFloat(fieldId);
      case BYTES -> getBytes(fieldId);
      case STRING -> getString(fieldId);
      case ARRAY -> getArray(fieldId);
      case OBJECT -> getObject(fieldId);
      case UTC_TIMESTAMP -> getInt(fieldId);
      case GEO_LOCATION -> getGeoLocation(fieldId);
      case GEO_LINE -> throw new UnsupportedOperationException();
      case GEO_POLYGON -> throw new UnsupportedOperationException();
    };
  }
}
