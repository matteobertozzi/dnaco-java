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

package tech.dnaco.storage.demo;

import java.util.Map;
import java.util.Map.Entry;

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.storage.demo.EntitySchema.Operation;

public class EntityDataRow {
  private final EntityDataRows rows;
  private final int rowIndex;

  public EntityDataRow(final EntityDataRows rows, final int rowIndex) {
    this.rows = rows;
    this.rowIndex = rowIndex;
  }

  public EntitySchema getSchema() {
    return rows.getSchema();
  }

  public boolean hasAllFields() {
    return rows.hasAllFields();
  }

  public void setSeqId(final long seqId) {
    rows.setSeqId(rowIndex, seqId);
  }

  public long getSeqId() {
    return rows.getSeqId(rowIndex);
  }

  public void setTimestamp(final long timestamp) {
    rows.setTimestamp(rowIndex, timestamp);
  }

  public long getTimestamp() {
    return rows.getTimestamp(rowIndex);
  }

  public void setOperation(final Operation operation) {
    rows.setOperation(rowIndex, operation);
  }

  public EntitySchema.Operation getOperation() {
    return rows.getOperation(rowIndex);
  }

  public byte[] get(final String fieldName) {
    return rows.get(rowIndex, fieldName);
  }

  public int size() {
    int size = 0;
    for (int i = 0, n = getSchema().fieldsCount(); i < n; ++i) {
      size += BytesUtil.length(rows.get(rowIndex, i));
    }
    return size;
  }

  public Object getObject(final String fieldName) {
    return rows.getObject(rowIndex, fieldName);
  }

  public EntityDataType getType(final String fieldName) {
    return rows.getSchema().getFieldType(fieldName);
  }

  public void mergeValues(final EntityDataRow other) {
    rows.mergeValues(rowIndex, other);
  }

  public byte[] buildRowKey() {
    return rows.buildRowKey(rowIndex);
  }

  public byte[] buildRowKey(final String txnId) {
    return rows.buildRowKey(rowIndex, txnId);
  }

  public static int compareKey(final EntityDataRow a, final EntityDataRow b) {
    for (final String keyField: a.getSchema().getKeyFields()) {
      final int cmp = BytesUtil.compare(a.get(keyField), b.get(keyField));
      if (cmp != 0) return cmp;
    }
    return 0;
  }

  public static int compareKeyAndSeq(final EntityDataRow a, final EntityDataRow b) {
    final int cmp = compareKey(a, b);
    return cmp != 0 ? cmp : Long.compare(b.getSeqId(), a.getSeqId());
  }

  public static EntityDataRow fromMap(final EntitySchema schema, final Map<String, Object> kvs) {
    final EntityDataRows rows = new EntityDataRows(schema, schema.userFieldsCount() == kvs.size()).newRow();
    for (final Entry<String, Object> entry: kvs.entrySet()) {
      rows.addObject(entry.getKey(), entry.getValue());
    }
    return new EntityDataRow(rows, 0);
  }

  @Override
  public String toString() {
    final EntitySchema schema = getSchema();
    final StringBuilder builder = new StringBuilder();
    builder.append(getSchema().getEntityName());
    builder.append("{");
    for (int fieldIndex = 0; fieldIndex < schema.fieldsCount(); ++fieldIndex) {
      if (fieldIndex > 0) builder.append(", ");
      final String field = schema.getFieldNames().get(fieldIndex);
      final EntityDataType type = schema.getFieldType(fieldIndex);
      final byte[] value = rows.get(rowIndex, fieldIndex);
      builder.append(field).append(":");
      if (value == null) {
        builder.append("null");
      } else if (schema.isKey(field)) {
        builder.append(new String(value));
      } else {
        builder.append(EntityData.decodeToObject(type, value));
      }
    }
    builder.append("}");
    return builder.toString();
  }
}
