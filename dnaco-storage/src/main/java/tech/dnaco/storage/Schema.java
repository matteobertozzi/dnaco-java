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
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ObjIntConsumer;
import java.util.regex.Pattern;

import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.logging.Logger;
import tech.dnaco.storage.DataTypes.Compatibility;
import tech.dnaco.storage.DataTypes.DataType;
import tech.dnaco.strings.StringUtil;

public final class Schema {
  private static final Pattern REGEX_FIELD_NAME = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

  private final AtomicLong serialId = new AtomicLong();

  // TODO: defaultValue
  private DataType[] fieldTypes = new DataType[0];
  private String[] fieldLabels = new String[0];
  private String[] fieldNames = new String[0];
  private int[] fieldsIndex = new int[0];
  private int[] fieldIds = new int[0];
  private int[] keys = new int[0];

  private int maxFieldId = 0;

  private long tableId;
  private final String name;
  private String label;

  public Schema() {
    this(null, 0);
  }

  public Schema(final String name, final long tableId) {
    this.name = name;
    this.tableId = tableId;
  }

  public Schema(final int[] fieldIds, final String[] fieldNames, final DataType[] fieldTypes) {
    this(null, null, fieldIds, ArrayUtil.max(fieldIds, 0, fieldIds.length) + 1, fieldNames, fieldTypes, fieldNames);
  }

  public Schema(final String name, final String label, final int[] fieldIds, final int maxFieldId,
      final String[] fieldNames, final DataType[] fieldTypes, final String[] fieldLabels) {
    this.name = name;
    this.label = label;
    this.fieldTypes = fieldTypes;
    this.fieldLabels = fieldLabels;
    this.fieldNames = fieldNames;
    this.fieldIds = fieldIds;
    this.maxFieldId = maxFieldId;

    this.fieldsIndex = buildFieldIndex();
  }

  public String getName() {
    return name;
  }

  public int fieldsCount() {
    return fieldNames.length;
  }

  public int maxFieldId() {
    return maxFieldId;
  }

  public int[] fieldIds() {
    return fieldIds;
  }

  public DataType[] fieldTypes() {
    return fieldTypes;
  }

  public String[] fieldNames() {
    return fieldNames;
  }

  public String[] fieldLabels() {
    return fieldLabels;
  }

  public int getFieldId(final int fieldIndex) {
    return fieldIds[fieldIndex];
  }

  public String getFieldName(final int fieldIndex) {
    return fieldNames[fieldIndex];
  }

  public DataType getFieldType(final int fieldIndex) {
    return fieldTypes[fieldIndex];
  }

  // ================================================================================
  //  Fields operations related
  // ================================================================================
  public boolean addField(final String name, final DataType type) {
    return addField(name, type, name);
  }

  public boolean addField(final String name, final DataType type, final String label) {
    validateFieldName(name);
    final int index = fieldByName(name);
    if (index >= 0) return false;

    final int fieldId = ++maxFieldId;
    final int insertionIndex = (-index - 1);
    fieldLabels = ArrayUtil.sortedInsert(fieldLabels, insertionIndex, label, String.class);
    fieldNames = ArrayUtil.sortedInsert(fieldNames, insertionIndex, name, String.class);
    fieldTypes = ArrayUtil.sortedInsert(fieldTypes, insertionIndex, type, DataType.class);
    fieldIds = ArrayUtil.sortedInsert(fieldIds, insertionIndex, fieldId);
    fieldsIndex = buildFieldIndex();
    return true;
  }

  public boolean removeField(final String name) {
    final int index = fieldByName(name);
    if (index < 0) return false;

    fieldLabels = ArrayUtil.sortedRemove(fieldLabels, index, String.class);
    fieldNames = ArrayUtil.sortedRemove(fieldNames, index, String.class);
    fieldTypes = ArrayUtil.sortedRemove(fieldTypes, index, DataType.class);
    fieldIds = ArrayUtil.sortedRemove(fieldIds, index);
    fieldsIndex = buildFieldIndex();
    return true;
  }

  public boolean renameField(final String oldName, final String newName) {
    validateFieldName(newName);
    final int index = fieldByName(oldName);
    if (index < 0) return false;

    fieldNames[index] = newName;
    return true;
  }

  public boolean updateFieldLabel(final String name, final String newLabel) {
    final int index = fieldByName(name);
    if (index < 0) return false;

    fieldLabels[index] = newLabel;
    return true;
  }

  public boolean updateFieldDataType(final String name, final DataType newType) {
    final int index = fieldByName(name);
    if (index < 0) return false;

    final Compatibility compatibility = DataTypes.checkCompatible(fieldTypes[index], newType);
    if (compatibility != Compatibility.NO_CHANGES) {
      Logger.error("cannot change field {} from {} to {}: {}", fieldTypes[index], newType, compatibility);
      return false;
    }

    fieldTypes[index] = newType;
    return true;
  }

  // ================================================================================
  //  Key Related
  // ================================================================================
  public boolean hasKey() {
    return ArrayUtil.isNotEmpty(keys);
  }

  public long nextSerialId() {
    return serialId.incrementAndGet();
  }

  public int isKey(final int fieldIndex) {
    for (int i = 0; i < keys.length; ++i) {
      if (keys[i] == fieldIndex) {
        return i;
      }
    }
    return -1;
  }

  public int[] keys() {
    return keys;
  }

  public void setKeys(final String[] fieldNames) {
    this.keys = new int[fieldNames.length];
    for (int i = 0; i < fieldNames.length; ++i) {
      keys[i] = fieldByName(fieldNames[i]);
    }
  }

  public int[] getFieldIdsByName(final String[] names) {
    final int[] index = new int[names.length];
    for (int i = 0; i < names.length; ++i) {
      index[i] = fieldIdByName(names[i]);
    }
    return index;
  }

  // ================================================================================
  //  Schema Mapping
  // ================================================================================
  public int[] mappingIndexesByName(final Schema other) {
    final int[] otherFieldIndexes = new int[fieldsIndex.length];
    Arrays.fill(otherFieldIndexes, -1);
    for (int i = 0; i < fieldIds.length; ++i) {
      otherFieldIndexes[fieldIds[i] - 1] = other.fieldByName(fieldNames[i]);
    }
    return otherFieldIndexes;
  }

  public static SchemaMapping map(final Schema fromSchema, final Schema toSchema) {
    final BitSet typeDifferences = new BitSet(toSchema.fieldsCount());
    final BitSet missingFields = new BitSet(toSchema.fieldsCount());
    final BitSet newFields = new BitSet(fromSchema.fieldsCount());

    final int[] mapping = new int[toSchema.fieldsCount()];
    Arrays.fill(mapping, -1);

    int toIndex = 0;
    int fromIndex = 0;
    while (toIndex < toSchema.fieldNames.length && fromIndex < fromSchema.fieldNames.length) {
      final int cmp = StringUtil.compare(toSchema.fieldNames[toIndex], fromSchema.fieldNames[fromIndex]);
      if (cmp == 0) {
        if (!toSchema.fieldTypes[toIndex].equals(fromSchema.fieldTypes[fromIndex])) {
          // B has the field but has a different type
          typeDifferences.set(toIndex);
        }
        mapping[toIndex] = fromIndex;
        toIndex++;
        fromIndex++;
      } else if (cmp < 0) {
        // B does not have a field that is in A
        missingFields.set(toIndex);
        toIndex++;
      } else {
        // B has a field that is not in A
        newFields.set(fromIndex);
        fromIndex++;
      }
    }

    while (toIndex < toSchema.fieldNames.length) {
      missingFields.set(toIndex);
      toIndex++;
    }

    while (fromIndex < fromSchema.fieldNames.length) {
      newFields.set(fromIndex);
      fromIndex++;
    }

    return new SchemaMapping(fromSchema, toSchema, mapping, typeDifferences, missingFields, newFields);
  }

  public static final class SchemaMapping {
    private final int[] fieldIndexMapping;
    private final BitSet typeDifferences;
    private final BitSet missingFields;
    private final BitSet newFields;
    private final Schema fromSchema;
    private final Schema toSchema;

    private SchemaMapping(final Schema fromSchema, final Schema toSchema,
        final int[] fieldIndexMapping, final BitSet typeDifferences,
        final BitSet missingFields, final BitSet newFields) {
      this.fieldIndexMapping = fieldIndexMapping;
      this.typeDifferences = typeDifferences.isEmpty() ? null : typeDifferences;
      this.missingFields = missingFields.isEmpty() ? null : missingFields;
      this.newFields = newFields.isEmpty() ? null : newFields;
      this.fromSchema = fromSchema;
      this.toSchema = toSchema;
    }

    public int[] fieldIndexMapping() {
      return fieldIndexMapping;
    }

    public boolean hasMissingFields() {
      return missingFields != null;
    }

    public boolean hasNewFields() {
      return newFields != null;
    }

    public void forEachMissingField(final ObjIntConsumer<Schema> consumer) {
      if (missingFields == null) return;
      for (int i = missingFields.nextSetBit(0); i >= 0; i = missingFields.nextSetBit(i+1)) {
        consumer.accept(toSchema, i);
      }
    }

    public void forEachNewField(final ObjIntConsumer<Schema> consumer) {
      if (newFields == null) return;
      for (int i = newFields.nextSetBit(0); i >= 0; i = newFields.nextSetBit(i+1)) {
        consumer.accept(fromSchema, i);
      }
    }

    public void forEachTypeDifference(final SchemaFieldDiffConsumer consumer) {
      if (typeDifferences == null) return;
      for (int i = typeDifferences.nextSetBit(0); i >= 0; i = typeDifferences.nextSetBit(i+1)) {
        consumer.accept(fromSchema, fieldIndexMapping[i], toSchema, i);
      }
    }

    @FunctionalInterface
    public interface SchemaFieldDiffConsumer {
      void accept(Schema fromSchema, int fromFieldIndex, Schema toSchema, int toFieldIndex);
    }
  }

  public static void main(final String[] args) {
    final Schema schemaA = new Schema();
    schemaA.addField("bool", DataType.BOOL);
    schemaA.addField("int", DataType.INT);
    schemaA.addField("float", DataType.FLOAT);
    schemaA.addField("string", DataType.STRING);

    final Schema schemaB = new Schema();
    schemaB.addField("string", DataType.STRING);
    schemaB.addField("float", DataType.FLOAT);
    schemaB.addField("int", DataType.INT);
    schemaB.addField("bool", DataType.BOOL);

    final Schema schemaC = new Schema();
    schemaC.addField("bool", DataType.BOOL);
    schemaC.addField("cool", DataType.BOOL);
    schemaC.addField("string", DataType.STRING);
    schemaC.addField("ttring", DataType.STRING);
    schemaC.addField("float", DataType.FLOAT);
    schemaC.addField("gloat", DataType.FLOAT);
    schemaC.addField("int", DataType.INT);
    schemaC.addField("lint", DataType.INT);

    final Schema schemaD = new Schema();
    schemaD.addField("bool", DataType.BOOL);
    schemaD.addField("cool", DataType.BOOL);
    schemaD.addField("string", DataType.BYTES);
    schemaD.addField("ttring", DataType.STRING);
    schemaD.addField("gloat", DataType.FLOAT);
    schemaD.addField("int", DataType.UTC_TIMESTAMP);
    schemaD.addField("lint", DataType.INT);

    final SchemaMapping mapping = map(schemaD, schemaA);
    mapping.forEachNewField((schema, index) -> System.out.println("NEW " + schema.getFieldName(index)));
    mapping.forEachMissingField((schema, index) -> System.out.println("MISSING " + schema.getFieldName(index)));
    mapping.forEachTypeDifference((fromSchema, fromIndex, toSchema, toIndex) -> System.out.println("DIFF " + fromSchema.getFieldType(fromIndex) + " -> " + toSchema.getFieldType(toIndex)));
  }

  // ================================================================================
  //  PRIVATE helpers
  // ================================================================================
  public int fieldByName(final String name) {
    return Arrays.binarySearch(fieldNames, name);
  }

  public int fieldIdByName(final String name) {
    return fieldIds[fieldByName(name)];
  }

  public int fieldIndexById(final int fieldId) {
    return fieldsIndex[fieldId - 1];
  }

  public boolean hasFieldId(final int fieldId) {
    return fieldId < fieldsIndex.length;
  }

  private int[] buildFieldIndex() {
    final int[] index = new int[maxFieldId];
    for (int i = 0; i < fieldIds.length; ++i) {
      index[fieldIds[i] - 1] = i;
    }
    return index;
  }

  private void validateFieldName(final String name) {
    if (!REGEX_FIELD_NAME.matcher(name).matches()) {
      throw new IllegalArgumentException("invalid field name: " + name);
    }
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("Schema [");
    for (int i = 0; i < fieldNames.length; ++i) {
      if (i > 0) builder.append(", ");
      builder.append(fieldIds[i]);
      builder.append(":");
      builder.append(fieldTypes[i]);
      builder.append(":");
      builder.append(fieldNames[i]);
    }
    builder.append("]");
    return builder.toString();
  }
}