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

package tech.dnaco.storage;

public final class DataTypes {
  public enum DataType {
    BOOL, INT, FLOAT, BYTES, STRING, ARRAY, OBJECT,
    UTC_TIMESTAMP,
    GEO_LOCATION, GEO_LINE, GEO_POLYGON,
  }

  private DataTypes() {
    // no-op
  }

  private static final DataType[] dataTypes = DataType.values();
  public static DataType typeFromId(final int id) {
    return dataTypes[id];
  }

  // ==========================================================================================
  //  Types Compatibility
  // ==========================================================================================
  public enum Compatibility { NO_CHANGES, REWRITE, NOT_COMPATIBLE }

  public static Compatibility checkCompatible(final DataType schemaType, final DataType valueType) {
    if (schemaType == valueType) return Compatibility.NO_CHANGES;

    // any type can be converted to a string with a rewrite
    if (valueType == DataType.STRING) return Compatibility.REWRITE;

    switch (schemaType) {
      case BOOL:
        // a boolean can be converted to a int with a rewrite
        return valueType == DataType.INT ? Compatibility.REWRITE : Compatibility.NOT_COMPATIBLE;
      case INT:
        // an int is compatible with the utc timestamp
        if (valueType == DataType.UTC_TIMESTAMP) return Compatibility.NO_CHANGES;
        // an int can be converted to a float without a rewrite
        if (valueType == DataType.FLOAT) return Compatibility.NO_CHANGES;
        // any other type is not compatible
        return Compatibility.NOT_COMPATIBLE;
      case FLOAT:
        // an float can be converted to an int with data loss
        if (valueType == DataType.INT) return Compatibility.REWRITE;
        // any other type is not compatible
        return Compatibility.NOT_COMPATIBLE;
      case UTC_TIMESTAMP:
        // an utc timestamp is compatible with an int
        if (valueType == DataType.INT) return Compatibility.NO_CHANGES;
        // any other type is not compatible
        return Compatibility.NOT_COMPATIBLE;
      default:
        // any other type is not compatible
        return Compatibility.NOT_COMPATIBLE;
    }
  }
}
