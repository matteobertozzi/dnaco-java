package tech.dnaco.storage.net.models;

import java.util.Arrays;

import tech.dnaco.collections.sets.HashIndexedArray;
import tech.dnaco.storage.demo.EntityDataRow;
import tech.dnaco.storage.demo.EntityDataRows;
import tech.dnaco.storage.demo.EntityDataType;
import tech.dnaco.storage.demo.EntitySchema;
import tech.dnaco.storage.demo.driver.AbstractKvStore.RowPredicate;

public class JsonEntityDataRows {
  private HashIndexedArray<String> fieldNames;
  private EntityDataType[] types;
  private Object[] values;

  public JsonEntityDataRows() {
    // no-op
  }

  public JsonEntityDataRows(final EntitySchema schema, final String[] fieldNames) {
    this.fieldNames = new HashIndexedArray<>(fieldNames);
    this.types = new EntityDataType[fieldNames.length];
    for (int i = 0; i < fieldNames.length; ++i) {
      this.types[i] = schema.getFieldType(fieldNames[i]);
    }
    this.values = new Object[0];
  }

  public String[] getFieldNames() {
    return fieldNames.keySet();
  }

  public EntityDataType[] getTypes() {
    return types;
  }

  public void add(final EntityDataRow row) {
    final String[] fields = fieldNames.keySet();
    final int rowOffset = values.length;
    this.values = Arrays.copyOf(values, rowOffset + fieldNames.size());
    for (int i = 0; i < fields.length; ++i) {
      values[rowOffset + i] = row.getObject(fields[i]);
    }
  }

  public boolean hasAllFields(final EntitySchema schema) {
    return schema.userFieldsCount() == fieldNames.size();
  }

  public boolean forEachEntityRow(final EntitySchema schema, final String[] groups,
      final RowPredicate consumer) throws Exception {
    final int fieldCount = fieldNames.size();
    for (int i = 0, n = rowCount(); i < n; ++i) {
      final int rowOffset = i * fieldCount;
      for (final String groupId: groups) {
        final EntityDataRows rows = new EntityDataRows(schema, schema.userFieldsCount() == fieldCount).newRow();
        rows.addObject(EntitySchema.SYS_FIELD_GROUP, groupId);
        rows.addObject(EntitySchema.SYS_FIELD_SEQID, Long.MAX_VALUE);
        for (int fieldIndex = 0; fieldIndex < fieldCount; ++fieldIndex) {
          rows.addObject(fieldNames.get(fieldIndex), values[rowOffset + fieldIndex]);
        }

        if (!consumer.test(new EntityDataRow(rows, 0))) {
          return false;
        }
      }
    }
    return true;
  }

  public void updateEntityRow(final EntityDataRows rows, final int rowIndex) {
    final int fieldCount = fieldNames.size();
    for (int fieldIndex = 0; fieldIndex < fieldCount; ++fieldIndex) {
      rows.addObject(fieldNames.get(fieldIndex), values[fieldIndex]);
    }
  }

  public int rowCount() {
    return values.length / fieldNames.size();
  }

  @Override
  public String toString() {
    return "JsonEntityDataRows [fieldNames=" + fieldNames + ", types=" + Arrays.toString(types) + ", values="
        + Arrays.toString(values) + "]";
  }
}
