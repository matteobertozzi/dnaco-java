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

import java.util.Arrays;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.encoding.RowKey;
import tech.dnaco.storage.DataTypes.DataType;
import tech.dnaco.storage.format.FieldFormat;
import tech.dnaco.storage.format.FieldFormatReader;

public class EntityRow {
  private static final Object DUMMY = new Object();

  private final Object[] values;
  private final Schema schema;

  private FieldFormatReader reader;

  public EntityRow(final Schema schema) {
    this.values = new Object[schema.fieldsCount()];
    this.schema = schema;
  }

  public Schema schema() {
    return schema;
  }

  public void load(final byte[] key, final byte[] value) {
    if (reader == null || reader.getVersion() != (value[0] & 0xff)) {
      reader = FieldFormat.newReader(schema, value[0] & 0xff);
    }
    // TODO: key
    reader.load(new ByteArraySlice(value, 1, value.length - 1));
    load(key, reader);
  }

  public void load(final byte[] key, final FieldFormatReader reader) {
    Arrays.fill(values, DUMMY);
    this.reader = reader;

    final int[] keys = schema.keys();
    final RowKey rowKey = new RowKey(key);
    for (int i = 0; i < keys.length; ++i) {
      values[keys[i]] = switch (schema.getFieldType(keys[i])) {
        case INT -> rowKey.getLong(i);
        case BOOL -> rowKey.getBool(i);
        case BYTES -> rowKey.get(i);
        case STRING -> rowKey.getString(i);
        case UTC_TIMESTAMP -> rowKey.getInt48(i);
        default -> throw new IllegalArgumentException("Unexpected value: " + schema.getFieldType(keys[i]));
      };
    }
  }

  public Object get(final int fieldIndex) {
    Object value = this.values[fieldIndex];
    if (value != DUMMY) {
      System.out.println("CACHE FIELD INDEX VALUE " + fieldIndex + " -> " + value);
      return value;
    }

    final DataType fieldType = schema.getFieldType(fieldIndex);
    final int fieldId = schema.getFieldId(fieldIndex);
    value = reader.get(fieldType, fieldId);
    this.values[fieldIndex] = value;
    System.out.println("COMPUTE FIELD INDEX VALUE " + fieldIndex + " -> " + value);
    return value;
  }

  public DataType getFieldType(final int fieldIndex) {
    return schema.getFieldType(fieldIndex);
  }
}
