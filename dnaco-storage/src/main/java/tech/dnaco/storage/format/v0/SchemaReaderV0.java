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

package tech.dnaco.storage.format.v0;

import java.nio.charset.StandardCharsets;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.encoding.VarInt;
import tech.dnaco.collections.LongValue;
import tech.dnaco.storage.DataTypes;
import tech.dnaco.storage.DataTypes.DataType;
import tech.dnaco.storage.Schema;
import tech.dnaco.storage.format.SchemaFormat.SchemaReader;

public final class SchemaReaderV0 implements SchemaReader {
  @Override
  public Schema readCompactSchema(final ByteArraySlice block, final int offset) {
    return readSchema(block, offset, true);
  }

  @Override
  public Schema readSchema(final ByteArraySlice block, final int offset) {
    return readSchema(block, offset, false);
  }

  private Schema readSchema(final ByteArraySlice block, int offset, final boolean compact) {
    final LongValue result = new LongValue();

    offset += VarInt.read(block, offset, result);
    final int fieldsCount = result.intValue();

    // read field IDs
    final int[] fieldIds = new int[fieldsCount];
    for (int i = 0; i < fieldsCount; ++i) {
      offset += VarInt.read(block, offset, result);
      fieldIds[i] = result.intValue();
    }

    // read field types
    final DataType[] fieldTypes = new DataType[fieldsCount];
    for (int i = 0; i < fieldsCount; ++i) {
      offset += VarInt.read(block, offset, result);
      fieldTypes[i] = DataTypes.typeFromId(result.intValue());
    }

    // read field names
    final String[] fieldNames = new String[fieldsCount];
    offset += readArrayOfString(block, offset, fieldNames);

    if (compact) {
      return new Schema(fieldIds, fieldNames, fieldTypes);
    }

    // read field labels
    final String[] fieldLabels = new String[fieldsCount];
    offset += readArrayOfString(block, offset, fieldLabels);

    // read max-fieldId
    offset += VarInt.read(block, offset, result);
    final int maxFieldId = result.intValue();

    // read name
    offset += VarInt.read(block, offset, result);
    int length = result.intValue();
    final String name = ByteArraySlice.newString(block, offset, length, StandardCharsets.UTF_8);

    // read label
    offset += VarInt.read(block, offset, result);
    length = result.intValue();
    final String label = ByteArraySlice.newString(block, offset, length, StandardCharsets.UTF_8);

    return new Schema(name, label, fieldIds, maxFieldId, fieldNames, fieldTypes, fieldLabels);
  }

  private static int readArrayOfString(final ByteArraySlice block, int offset, final String[] items) {
    final LongValue result = new LongValue();
    for (int i = 0; i < items.length; ++i) {
      offset += VarInt.read(block, offset, result);
      final int length = result.intValue();
      items[i] = ByteArraySlice.newString(block, offset, length, StandardCharsets.UTF_8);
      offset += length;
    }
    return offset;
  }
}
