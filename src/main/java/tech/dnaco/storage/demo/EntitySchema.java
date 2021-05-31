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
  private String label;
  private long mtime;
  private boolean sync;

  public EntitySchema(final String entityName) {
    this(entityName, entityName, System.currentTimeMillis());
  }

  public EntitySchema(final String entityName, final String label, final long mtime) {
    this.name = entityName;
    this.label = entityName;
    this.mtime = mtime;
    this.update(SYS_FIELD_GROUP, EntityDataType.STRING);
    this.update(SYS_FIELD_SEQID, EntityDataType.INT);
    this.update(SYS_FIELD_TIMESTAMP, EntityDataType.INT);
    this.update(SYS_FIELD_OPERATION, EntityDataType.INT);
  }

  public int fieldsCount() {
    return fields.size();
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

  public List<String> getFieldNames() {
    return fields.keys();
  }

  public String[] getKeyFields() {
    return keys != null ? keys.keySet() : StringUtil.EMPTY_ARRAY;
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

  public boolean setKey(final String[] newKeys) {
    if (keys == null) {
      keys = newKeys != null ? new HashIndexedArray<>(newKeys) : null;
    }
    return newKeys != null ? Arrays.equals(keys.keySet(), newKeys) : (keys == null);
  }


  public boolean remove(final String fieldName) {
    final int index = fields.remove(fieldName);
    if (index < 0) return false;

    final List<String> keys = fields.keys();
    this.fields = new IndexedHashSet<>();
    this.fields.addAll(keys);

    types.remove(index);
    return true;
  }

  public boolean update(final String fieldName, final EntityDataType type) {
    final int index = fields.add(fieldName);
    if (index == this.types.size()) {
      this.types.add(type);
      return true;
    }

    final EntityDataType currentType = this.types.get(index);
    if (getTypeCompatibility(type, currentType) < 0) {
      Logger.error("type mismatch for entity {} field {}. expected {} got {}",
        name, fieldName, this.types.get(index), type);
      return false;
    }

    if (type != EntityDataType.NULL) {
      this.types.set(index, type);
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

  public static void main(final String[] args) {
    System.out.println(getTypeCompatibility(EntityDataType.NULL, EntityDataType.NULL));
    System.out.println(getTypeCompatibility(EntityDataType.NULL, EntityDataType.STRING));
    System.out.println(getTypeCompatibility(EntityDataType.STRING, EntityDataType.NULL));
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
        return type == EntityDataType.INT ? 0 : (type == EntityDataType.FLOAT ? 1 : -1);
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
    json.add("keys", keys != null ? JsonUtil.toJsonTree(keys.keySet()) : null);
    json.add("types", JsonUtil.toJsonTree(types));
    json.add("fields", JsonUtil.toJsonTree(fields.keys()));
    return EntityData.encodeJsonObject(json);
  }

  public static EntitySchema decode(final byte[] rawValue) {
    return decode(new ByteArraySlice(rawValue));
  }

  public static EntitySchema decode(final ByteArraySlice rawValue) {
    final JsonObject json = (JsonObject) EntityData.decodeJsonObject(rawValue);
    if (json == null) throw new IllegalArgumentException();

    final String name = json.get("name").getAsString();
    final String label = JsonUtil.getString(json, "label", null);
    final long mtime = JsonUtil.getLong(json, "mtime", 0);
    final boolean sync = JsonUtil.getBoolean(json, "sync", false);

    final EntitySchema schema = new EntitySchema(name, label, mtime);
    final EntityDataType[] types = JsonUtil.fromJson(json.get("types"), EntityDataType[].class);
    final String[] fields = JsonUtil.fromJson(json.get("fields"), String[].class);
    schema.setSync(sync);
    schema.update(fields, types);
    schema.setKey(JsonUtil.fromJson(json.get("keys"), String[].class));
    return schema;
  }
}
