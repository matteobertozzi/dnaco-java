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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.gullivernet.commons.util.VerifyArg;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.storage.demo.EntitySchema.Operation;
import tech.dnaco.storage.demo.RowKeyUtil.RowKeyBuilder;
import tech.dnaco.strings.StringUtil;

public class EntityDataRows {
  private final ArrayList<byte[]> values = new ArrayList<>();
  private final EntitySchema schema;

  public EntityDataRows(final EntitySchema schema) {
    this.schema = VerifyArg.verifyNotNull("schema", schema);
  }

  public EntitySchema getSchema() {
    return schema;
  }

  public boolean isEmpty() {
    return values.isEmpty();
  }

  public boolean isNotEmpty() {
    return !values.isEmpty();
  }

  public int rowCount() {
    return values.size() / schema.fieldsCount();
  }

  public EntityDataRows newRow() {
    for (int i = 0, n = schema.fieldsCount(); i < n; ++i) {
      values.add(null);
    }
    return this;
  }

  public void addKey(final List<byte[]> keyParts) {
    final String[] keyFields = schema.getKeyFields();
    add(EntitySchema.SYS_FIELD_GROUP, keyParts.get(0));
    for (int i = 0; i < keyFields.length; ++i) {
      add(keyFields[i], keyParts.get(2 + i));
    }
  }

  public void addTxnKey(final List<byte[]> keyParts) {
    final String[] keyFields = schema.getKeyFields();
    add(EntitySchema.SYS_FIELD_GROUP, keyParts.get(1));
    for (int i = 0; i < keyFields.length; ++i) {
      add(keyFields[i], keyParts.get(3 + i));
    }
  }

  public void add(final int fieldIndex, final byte[] value) {
    final int index = (values.size() - schema.fieldsCount()) + fieldIndex;
    values.set(index, value);
  }

  public void add(final String fieldName, final byte[] value) {
    final int fieldIndex = schema.getFieldIndex(fieldName);
    add(fieldIndex, value);
  }

  public void addObject(final String fieldName, final Object value) {
    final int fieldIndex = schema.getFieldIndex(fieldName);
    if (schema.isKey(fieldName)) {
      final EntityDataType type = schema.getFieldType(fieldIndex);
      switch (type) {
        case STRING:
          add(fieldIndex, ((String)value).getBytes());
          break;
        case INT:
          if (value instanceof Integer) {
            add(fieldIndex, String.valueOf(value).getBytes());
          } else if (value instanceof Double) {
            add(fieldIndex, String.valueOf(Math.round((Double)value)).getBytes());
          } else {
            throw new UnsupportedOperationException("expected/int/double for field " + fieldName + " got " + value);
          }
          break;
        default:
          throw new UnsupportedOperationException();
      }
    } else {
      add(fieldIndex, EntityData.encodeFromObject(schema.getFieldType(fieldIndex), value));
    }
  }

  public void copyFrom(final EntityDataRow oldRow) {
    for (final String fieldName: schema.getFieldNames()) {
      add(fieldName, oldRow.get(fieldName));
    }
  }

  public void copyFrom(final EntityDataRows oldRow) {
    for (int i = 0; i < oldRow.values.size(); ++i) {
      values.set(i, oldRow.values.get(i));
    }
  }

  public void setSeqId(final int rowIndex, final long seqId) {
    final int fieldIndex = schema.getFieldIndex(EntitySchema.SYS_FIELD_SEQID);
    final int rowOffset = rowIndex * schema.fieldsCount();
    values.set(rowOffset + fieldIndex, EntityData.encodeInt(seqId));
  }

  public long getSeqId(final int rowIndex) {
    final byte[] v = get(rowIndex, EntitySchema.SYS_FIELD_SEQID);
    final Number seqId = (Number) EntityData.decodeInt(new ByteArraySlice(v));
    return seqId.longValue();
  }

  public void setOperation(final int rowIndex, final Operation operation) {
    final int fieldIndex = schema.getFieldIndex(EntitySchema.SYS_FIELD_OPERATION);
    final int rowOffset = rowIndex * schema.fieldsCount();
    values.set(rowOffset + fieldIndex, EntityData.encodeInt(operation.ordinal()));
  }

  public EntitySchema.Operation getOperation(final int rowIndex) {
    final byte[] v = get(rowIndex, EntitySchema.SYS_FIELD_OPERATION);
    final Number ordinal = (Number) EntityData.decodeInt(new ByteArraySlice(v));
    return EntitySchema.Operation.values()[ordinal.intValue()];
  }

  public void setTimestamp(final int rowIndex, final long timestamp) {
    final int fieldIndex = schema.getFieldIndex(EntitySchema.SYS_FIELD_TIMESTAMP);
    final int rowOffset = rowIndex * schema.fieldsCount();
    values.set(rowOffset + fieldIndex, EntityData.encodeInt(timestamp));
  }

  public long getTimestamp(final int rowIndex) {
    final byte[] v = get(rowIndex, EntitySchema.SYS_FIELD_TIMESTAMP);
    return (long) EntityData.decodeInt(new ByteArraySlice(v));
  }

  public byte[] get(final int rowIndex, final String fieldName) {
    final int fieldIndex = schema.getFieldIndex(fieldName);
    return fieldIndex < 0 ? null : get(rowIndex, fieldIndex);
  }

  public byte[] get(final int rowIndex, final int fieldIndex) {
    final int rowOffset = rowIndex * schema.fieldsCount();
    return values.get(rowOffset + fieldIndex);
  }

  public Object getObject(final int rowIndex, final String fieldName) {
    final int fieldIndex = schema.getFieldIndex(fieldName);
    return fieldIndex < 0 ? null : getObject(rowIndex, fieldIndex);
  }

  public Object getObject(final int rowIndex, final int fieldIndex) {
    final byte[] value = get(rowIndex, fieldIndex);
    if (value == null) return null;

    final EntityDataType type = schema.getFieldType(fieldIndex);
    if (schema.isKey(fieldIndex)) {
      switch (type) {
        case STRING:
          return new String(value);
        case INT:
          return Long.parseLong(new String(value));
        default:
          throw new UnsupportedOperationException();
      }
    }
    return EntityData.decodeToObject(type, value);
  }

  public void mergeValues(final int rowIndex, final EntityDataRow other) {
    final int rowOffset = rowIndex * schema.fieldsCount();
    for (final String fieldName: schema.getNonKeyFields()) {
      final int fieldIndex = schema.getFieldIndex(fieldName);
      if (values.get(rowOffset + fieldIndex) == null) {
        values.set(rowOffset + fieldIndex, other.get(fieldName));
      }
    }
  }

  public static final String SYS_TXN_PREFIX_STR = "__SYS_TXN__.";
  public static final byte[] SYS_TXN_PREFIX = SYS_TXN_PREFIX_STR.getBytes(StandardCharsets.UTF_8);
  public static ByteArraySlice buildTxnRowPrefix(final String txnId) {
    final RowKeyBuilder builder = RowKeyUtil.newKeyBuilder();
    builder.add(SYS_TXN_PREFIX_STR + txnId);
    builder.addKeySeparator();
    return builder.slice();
  }

  public static ByteArraySlice addTxnToRowPrefix(final String txnId, final ByteArraySlice prefix) {
    final RowKeyBuilder builder = RowKeyUtil.newKeyBuilder();
    builder.add(SYS_TXN_PREFIX_STR + txnId);
    builder.add(prefix.buffer());
    return builder.slice();
  }

  public byte[] buildRowKey(final int rowIndex) {
    return buildRowKey(rowIndex, null);
  }

  public byte[] buildRowKey(final int rowIndex, final String txnId) {
    final int rowOffset = rowIndex * schema.fieldsCount();

    final RowKeyBuilder builder = new RowKeyBuilder();
    if (StringUtil.isNotEmpty(txnId)) builder.add(SYS_TXN_PREFIX_STR + txnId);
    builder.add(values.get(rowOffset + schema.getFieldIndex(EntitySchema.SYS_FIELD_GROUP)));
    builder.add(schema.getEntityName());
    for (final String key: schema.getKeyFields()) {
      final int keyIndex = schema.getFieldIndex(key);
      builder.add(values.get(rowOffset + keyIndex));
    }
    return builder.drain();
  }

  public String toString() {
    if (isEmpty()) {
      return schema.getEntityName() + "[]";
    }

    final StringBuilder builder = new StringBuilder();
    builder.append(schema.getEntityName()).append("[");
    for (int i = 0, n = rowCount(); i < n; ++i) {
      if (i > 0) builder.append(", ");

      builder.append("{");
      final int rowIndex = i * schema.fieldsCount();
      for (int fieldIndex = 0; fieldIndex < schema.fieldsCount(); ++fieldIndex) {
        if (fieldIndex > 0) builder.append(", ");
        final String field = schema.getFieldNames().get(fieldIndex);
        final EntityDataType type = schema.getFieldType(fieldIndex);
        final byte[] value = values.get(rowIndex + fieldIndex);
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
    }
    builder.append("]");
    return builder.toString();
  }
}
