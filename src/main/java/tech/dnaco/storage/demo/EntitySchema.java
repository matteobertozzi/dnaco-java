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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.collections.sets.HashIndexedArray;
import tech.dnaco.collections.sets.IndexedHashSet;
import tech.dnaco.data.json.JsonObject;
import tech.dnaco.data.json.JsonUtil;
import tech.dnaco.logging.Logger;
import tech.dnaco.storage.net.EntityStorageScheduled.TableStats;
import tech.dnaco.storage.net.models.ClientSchema;
import tech.dnaco.storage.net.models.ClientSchema.EntityField;
import tech.dnaco.strings.StringFormat;
import tech.dnaco.strings.StringUtil;

public class EntitySchema {
  public enum Operation { INSERT, UPSERT, UPDATE, DELETE }

  public static final String SYS_TXN_PREFIX = "__SYS_TXN__.";
  public static final String SYS_FIELD_GROUP = "__group__";
  public static final String SYS_FIELD_SEQID = "__seqId__";
  public static final String SYS_FIELD_OPERATION = "__op__";
  public static final String SYS_FIELD_TIMESTAMP = "__ts__";

  private IndexedHashSet<String> fields = new IndexedHashSet<>();
  private final ArrayList<EntityDataType> types = new ArrayList<>();
  private final String name;

  private HashIndexedArray<String> keys;
  private String dataType;
  private String label;
  private long mtime;
  private long retentionPeriod;
  private boolean sync;

  public EntitySchema(final String entityName) {
    this(entityName, entityName, System.currentTimeMillis(), null, 0);
  }

  public EntitySchema(final String entityName, final String label, final long mtime,
      final String dataType, final long retentionPeriod) {
    this.name = entityName;
    this.label = label;
    this.mtime = mtime;
    this.dataType = dataType;
    this.retentionPeriod = retentionPeriod;
    this.update(SYS_FIELD_GROUP, EntityDataType.STRING);
    this.update(SYS_FIELD_SEQID, EntityDataType.INT);
    this.update(SYS_FIELD_TIMESTAMP, EntityDataType.INT);
    this.update(SYS_FIELD_OPERATION, EntityDataType.INT);
  }

  public ClientSchema toClientJson(final TableStats stats, final boolean includeFields) {

    final ClientSchema schema = new ClientSchema();
    schema.setName(name);
    schema.setLabel(label);
    schema.setEditTime(mtime);
    schema.setSync(sync);
    schema.setDataType(dataType);
    schema.setRetentionPeriod(retentionPeriod);
    schema.setRowCount(stats.getRowCount());
    schema.setDiskUsage(stats.getDiskUsage());
    schema.setGroups(stats.getGroups());

    if (includeFields) {
      final ArrayList<EntityField> jsonKey = new ArrayList<>(keys.size());
      for (int i = 0; i < keys.size(); ++i) {
        final String fieldName = keys.get(i);
        final int index = fields.get(fieldName);
        final EntityField jsonField = new EntityField();
        jsonField.setType(types.get(index).name());
        jsonField.setName(fieldName);
        jsonField.setKey(true);
        jsonField.setDiskUsage(stats.getDiskUsage(fieldName));
        jsonKey.add(jsonField);
      }

      final ArrayList<EntityField> jsonFields = new ArrayList<>(types.size() - 4);
      for (int i = 4, n = fields.size(); i < n; ++i) {
        final String fieldName = fields.get(i);
        if (isKey(fieldName)) continue;

        final EntityField jsonField = new EntityField();
        jsonField.setType(types.get(i).name());
        jsonField.setName(fieldName);
        jsonField.setKey(false);
        jsonField.setDiskUsage(stats.getDiskUsage(fieldName));
        jsonFields.add(jsonField);
      }
      jsonFields.sort((a, b) -> {
        final int cmp = a.getType().compareTo(b.getType());
        if (cmp != 0) return cmp;
        return a.getName().compareTo(b.getName());
      });

      jsonFields.addAll(0, jsonKey);

      schema.setFields(jsonFields.toArray(new EntityField[0]));
    }

    return schema;
  }

  public int fieldsCount() {
    return fields.size();
  }

  public int userFieldsCount() {
    return fields.size() - 4;
  }

  public int getFieldIndex(final String name) {
    return fields.get(name);
  }

  public EntityDataType getFieldType(final int fieldIndex) {
    return types.get(fieldIndex);
  }

  public EntityDataType getFieldType(final String name) {
    final int fieldIndex = getFieldIndex(name);
    return fieldIndex < 0 ? null : getFieldType(fieldIndex);
  }

  public byte[] getDefaultValue(final int fieldIndex) {
    return EntityData.encodeNull();
  }

  public String getEntityName() {
    return name;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(final String label) {
    this.label = label;
  }

  public long getModificationTime() {
    return mtime;
  }

  public void setModificationTime(final long mtime) {
    this.mtime = mtime;
  }

  public boolean getSync() {
    return sync;
  }

  public void setSync(final boolean sync) {
    this.sync = sync;
  }

  public String getDataType() {
    return dataType;
  }

  public void setDataType(final String dataType) {
    this.dataType = dataType;
  }

  public long getRetentionPeriod() {
    return retentionPeriod;
  }

  public void setRetentionPeriod(final long period) {
    this.retentionPeriod = period;
  }

  public List<String> getFieldNames() {
    return fields.keys();
  }

  public String getFieldName(final int index) {
    return fields.get(index);
  }

  public String[] getKeyFields() {
    return keys != null ? keys.keySet() : StringUtil.EMPTY_ARRAY;
  }

  public int keyFieldsCount() {
    return keys != null ? keys.size() : 0;
  }

  public int nonKeyFieldsCount() {
    return keys != null ? fields.size() - keys.size() : fields.size();
  }

  public List<String> getNonKeyFields() {
    final ArrayList<String> nonKeyFields = new ArrayList<>(fields.size());
    for (final String field: this.fields.keys()) {
      if (!isKey(field)) {
        nonKeyFields.add(field);
      }
    }
    return nonKeyFields;
  }

  public boolean isSysField(final String name) {
    switch (name) {
      case SYS_FIELD_OPERATION:
      case SYS_FIELD_TIMESTAMP:
      case SYS_FIELD_SEQID:
      case SYS_FIELD_GROUP:
        return true;
    }
    return false;
  }

  public boolean isKey(final int fieldIndex) {
    return isKey(fields.get(fieldIndex));
  }

  public boolean isKey(final String fieldName) {
    if (fieldName == null) return false;
    if (fieldName.equals(SYS_FIELD_GROUP)) return true;
    return keys != null && keys.contains(fieldName);
  }

  public synchronized boolean setKey(final String[] newKeys) {
    if (keys == null) {
      keys = newKeys != null ? new HashIndexedArray<>(newKeys) : null;
    }
    return newKeys != null ? Arrays.equals(keys.keySet(), newKeys) : (keys == null);
  }


  public synchronized boolean remove(final String fieldName) {
    final int index = fields.remove(fieldName);
    if (index < 0) return false;

    final List<String> keys = fields.keys();
    this.fields = new IndexedHashSet<>();
    this.fields.addAll(keys);

    types.remove(index);
    return true;
  }

  public synchronized boolean update(final String fieldName, final EntityDataType type) {
    final int index = fields.add(fieldName);
    if (index == this.types.size()) {
      this.types.add(type);
      return true;
    }

    final EntityDataType currentType = this.types.get(index);
    if (getTypeCompatibility(type, currentType) < 0) {
      Logger.error("type mismatch for entity {} field {}. expected {} got {}",
        name, fieldName, currentType, type);
      throw new IllegalArgumentException(StringFormat.format("type mismatch for entity {} field {}. expected {} got {}",
        name, fieldName, currentType, type));
    }

    if (type != EntityDataType.NULL) {
      if (currentType == EntityDataType.FLOAT && type == EntityDataType.INT) {
        // skip assignment. float is bigger
      } else {
        this.types.set(index, type);
      }
    }
    return true;
  }

  public boolean update(final String[] fieldNames, final EntityDataType[] types) {
    for (int i = 0; i < fieldNames.length; ++i) {
      if (!update(fieldNames[i], types[i])) {
        return false;
      }
    }
    return true;
  }

  public static int getTypeCompatibility(final EntityDataType type, final EntityDataType expectedType) {
    switch (expectedType) {
      case NULL:
        // any type is a super type? bool/int/float can be nullable?
        return 1;
      case BOOL:
        return type != EntityDataType.BOOL ? -1 : 0;
      case INT:
        // only a float is a super-type
        return type != EntityDataType.INT ? -1 : 0;
        //return type == EntityDataType.INT ? 0 : (type == EntityDataType.FLOAT ? 1 : -1);
      case FLOAT:
        // consider int and float as same type
        return (type == EntityDataType.FLOAT || type == EntityDataType.INT) ? 0 : -1;
      case BYTES:
        // null is also acceptable
        return (type == EntityDataType.BYTES || type == EntityDataType.NULL) ? 0 : -1;
      case STRING:
        // null is also acceptable
        return (type == EntityDataType.STRING || type == EntityDataType.NULL) ? 0 : -1;
      case UTC_TIMESTAMP:
        return (type == EntityDataType.UTC_TIMESTAMP || type == EntityDataType.INT) ? 0 : -1;
      case JSON_ARRAY:
        // null is also acceptable
        return (type == EntityDataType.JSON_ARRAY || type == EntityDataType.NULL) ? 0 : -1;
      case JSON_OBJECT:
        // null is also acceptable
        return (type == EntityDataType.JSON_OBJECT || type == EntityDataType.NULL) ? 0 : -1;
    }
    throw new UnsupportedOperationException("unexpected type: " + expectedType);
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("EntitySchema [name=").append(name);
    builder.append(", label=").append(label);
    builder.append(", sync=").append(sync);
    builder.append(", dataType=").append(dataType);
    builder.append(", retentionPeriod=").append(retentionPeriod);
    builder.append(", {");
    for (int i = 0, n = fields.size(); i < n; ++i) {
      if (i > 0) builder.append(", ");
      builder.append(fields.get(i)).append(":");
      builder.append(types.get(i));
    }
    builder.append("}]");
    return builder.toString();
  }

  public byte[] encode() {
    final JsonObject json = new JsonObject();
    json.addProperty("name", name);
    json.addProperty("label", label);
    json.addProperty("dataType", dataType);
    json.addProperty("mtime", mtime);
    json.addProperty("retentionPeriod", retentionPeriod);
    json.addProperty("sync", sync);

    json.add("keys", keys != null ? JsonUtil.toJsonTree(keys.keySet()) : null);
    json.add("types", JsonUtil.toJsonTree(types));
    json.add("fields", JsonUtil.toJsonTree(fields.keys()));
    return EntityData.encodeJsonObject(json);
  }

  public static EntitySchema decode(final byte[] rawValue) {
    return decode(new ByteArraySlice(rawValue));
  }

  private static EntitySchema decode(final ByteArraySlice rawValue) {
    final JsonObject json = (JsonObject) EntityData.decodeJsonObject(rawValue);
    if (json == null) throw new IllegalArgumentException();

    final String name = json.get("name").getAsString();
    final String label = JsonUtil.getString(json, "label", null);
    final String dataType = JsonUtil.getString(json, "dataType", null);
    final long mtime = JsonUtil.getLong(json, "mtime", 0);
    final long retentionPeriod = JsonUtil.getLong(json, "retentionPeriod", 0);
    final boolean sync = JsonUtil.getBoolean(json, "sync", false);

    final EntitySchema schema = new EntitySchema(name, label, mtime, dataType, retentionPeriod);
    final EntityDataType[] types = JsonUtil.fromJson(json.get("types"), EntityDataType[].class);
    final String[] fields = JsonUtil.fromJson(json.get("fields"), String[].class);
    schema.setSync(sync);
    schema.update(fields, types);
    schema.setKey(JsonUtil.fromJson(json.get("keys"), String[].class));
    return schema;
  }
}
