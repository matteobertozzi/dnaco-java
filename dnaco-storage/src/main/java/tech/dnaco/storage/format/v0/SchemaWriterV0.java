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

import tech.dnaco.bytes.encoding.VarInt;
import tech.dnaco.collections.arrays.paged.PagedByteArray;
import tech.dnaco.storage.DataTypes.DataType;
import tech.dnaco.storage.Schema;
import tech.dnaco.storage.format.SchemaFormat.SchemaWriter;

public final class SchemaWriterV0 implements SchemaWriter {
  @Override
  public void writeCompactSchema(final Schema schema, final PagedByteArray buffer) {
    VarInt.write(buffer, schema.fieldsCount());
    for (final int fieldId: schema.fieldIds()) {
      VarInt.write(buffer, fieldId);
    }
    for (final DataType fieldType: schema.fieldTypes()) {
      VarInt.write(buffer, fieldType.ordinal());
    }
    for (final String fieldName: schema.fieldNames()) {
      buffer.addBlob(fieldName);
    }
  }

  @Override
  public void writeSchema(final Schema schema, final PagedByteArray buffer) {
    for (final String fieldLabel: schema.fieldLabels()) {
      buffer.addBlob(fieldLabel);
    }

    // schema info
    VarInt.write(buffer, schema.maxFieldId());
    buffer.addBlob(schema.getName());
  }
}
