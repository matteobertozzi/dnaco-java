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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.data.json.JsonArray;
import tech.dnaco.data.json.JsonObject;
import tech.dnaco.data.json.JsonUtil;
import tech.dnaco.geo.LatLong;
import tech.dnaco.storage.DataTypes.DataType;
import tech.dnaco.storage.Schema.SchemaMapping;
import tech.dnaco.storage.format.FieldFormatReader;
import tech.dnaco.storage.format.FieldFormatWriter;

public class EntityRowConverter {
  private final int[] indexMapping;
  private final Schema rowSchema;
  private final Schema schema;

  public EntityRowConverter(final Schema fromSchema, final Schema toSchema) {
    this.rowSchema = fromSchema;
    this.schema = toSchema;
    final SchemaMapping mapping = Schema.map(fromSchema, toSchema);
    this.indexMapping = mapping.fieldIndexMapping();
    System.out.println("SOURCE: " + rowSchema);
    System.out.println("TARGET: " + schema);
    System.out.println("mapping: " + Arrays.toString(indexMapping));
  }

  public void convertKey(final FieldFormatReader rowReader, final RowKey.RowKeyBuilder key) {
    final int[] fieldIds = schema.fieldIds();
    final int[] keys = schema.keys();
    for (int i = 0; i < keys.length; ++i) {
      final int targetFieldId = fieldIds[keys[i]];
      final int otherIndex = indexMapping[targetFieldId - 1];
      if (otherIndex < 0) throw new IllegalArgumentException("missing key");

      final int sourceFieldId = rowSchema.getFieldId(otherIndex);
      switch (schema.getFieldType(keys[i])) {
        case INT -> key.addInt(rowReader.getInt(sourceFieldId));
        case BOOL -> key.addBool(rowReader.getBool(sourceFieldId));
        case BYTES -> key.add(rowReader.getBytes(sourceFieldId));
        case STRING -> key.add(rowReader.getString(sourceFieldId));
        case UTC_TIMESTAMP -> key.addInt48(rowReader.getInt(sourceFieldId));
        default -> throw new IllegalArgumentException("Unexpected key value: " + schema.getFieldType(keys[i]));
      }
    }
  }

  public void convert(final FieldFormatReader rowReader, final FieldFormatWriter writer) {
    final int[] fieldIds = schema.fieldIds();
    for (int i = 0; i < fieldIds.length; ++i) {
      final int targetFieldId = fieldIds[i];
      final int otherIndex = indexMapping[targetFieldId - 1];
      //System.out.println("REWRITE " + (targetFieldId + " -> " + i) + "/" + (" -> " + otherIndex));
      if (otherIndex < 0) continue;

      final int sourceFieldId = rowSchema.getFieldId(otherIndex);
      if (rowReader.isNull(sourceFieldId)) {
        writer.writeNull(targetFieldId);
        return;
      }

      final DataType sourceType = rowSchema.getFieldType(otherIndex);
      final DataType targetType = schema.getFieldType(i);
      if (sourceType == targetType) {
        copyField(rowReader, writer, sourceType, sourceFieldId, targetFieldId);
      } else {
        convertField(rowReader, sourceType, sourceFieldId, writer, targetType, targetFieldId);
      }
    }
  }

  private static void copyField(final FieldFormatReader reader, final FieldFormatWriter writer,
      final DataType type, final int sourceFieldId, final int targetFieldId) {
    switch (type) {
      case BOOL -> writer.writeBool(targetFieldId, reader.getBool(sourceFieldId));
      case INT, UTC_TIMESTAMP -> writer.writeInt(targetFieldId, reader.getInt(sourceFieldId));
      case FLOAT -> writer.writeFloat(targetFieldId, reader.getFloat(sourceFieldId));
      case BYTES -> writer.writeBytes(targetFieldId, reader.getBytes(sourceFieldId));
      case STRING -> writer.writeString(targetFieldId, reader.getString(sourceFieldId));
      case ARRAY -> writer.writeArray(targetFieldId, reader.getArray(sourceFieldId));
      case OBJECT -> writer.writeObject(targetFieldId, reader.getObject(sourceFieldId));
      case GEO_LOCATION -> writer.writeGeoLocation(targetFieldId, reader.getGeoLocation(sourceFieldId));
      case GEO_LINE -> throw new UnsupportedOperationException();
      case GEO_POLYGON -> throw new UnsupportedOperationException();
    }
  }

  private static void convertField(final FieldFormatReader sourceReader, final DataType sourceType, final int sourceFieldId,
      final FieldFormatWriter targetWriter, final DataType targetType, final int targetFieldId) {
    switch (sourceType) {
      case BOOL: {
        final boolean value = sourceReader.getBool(sourceFieldId);
        switch (targetType) {
          case INT -> targetWriter.writeInt(targetFieldId, value ? 1 : 0);
          case STRING -> targetWriter.writeString(targetFieldId, String.valueOf(value));
          default -> throw new UnsupportedOperationException();
        }
        return;
      }
      case UTC_TIMESTAMP:
      case INT: {
        final long value = sourceReader.getInt(sourceFieldId);
        switch (targetType) {
          case FLOAT, UTC_TIMESTAMP -> targetWriter.writeInt(targetFieldId, value);
          case STRING -> targetWriter.writeString(targetFieldId, String.valueOf(value));
          default -> throw new UnsupportedOperationException();
        }
        return;
      }
      case FLOAT: {
        final double value = sourceReader.getFloat(sourceFieldId);
        switch (targetType) {
          case INT -> targetWriter.writeInt(targetFieldId, Math.round(value));
          case STRING -> targetWriter.writeString(targetFieldId, String.valueOf(value));
          default -> throw new UnsupportedOperationException();
        }
        return;
      }
      case BYTES: {
        final ByteArraySlice value = sourceReader.getBytes(sourceFieldId);
        if (targetType == DataType.STRING) {
          targetWriter.writeString(targetFieldId, new String(value.rawBuffer(), value.offset(), value.length(), StandardCharsets.UTF_8));
        } else {
          throw new UnsupportedOperationException();
        }
        return;
      }
      case ARRAY: {
        final JsonArray array = sourceReader.getArray(sourceFieldId);
        if (targetType == DataType.STRING) {
          targetWriter.writeString(targetFieldId, array.toString());
        } else {
          throw new UnsupportedOperationException();
        }
        return;
      }
      case OBJECT: {
        final JsonObject object = sourceReader.getObject(sourceFieldId);
        if (targetType == DataType.STRING) {
          targetWriter.writeString(targetFieldId, object.toString());
        } else {
          throw new UnsupportedOperationException();
        }
        return;
      }
      case GEO_LOCATION:
        final LatLong latlng = sourceReader.getGeoLocation(sourceFieldId);
        if (targetType == DataType.STRING) {
          targetWriter.writeString(targetFieldId, JsonUtil.toJson(latlng));
        } else {
          throw new UnsupportedOperationException();
        }
        return;
      default:
        break;
    }
    throw new UnsupportedOperationException();
  }
}
