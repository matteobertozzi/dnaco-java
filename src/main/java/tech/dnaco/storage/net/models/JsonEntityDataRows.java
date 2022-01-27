package tech.dnaco.storage.net.models;

import java.util.Arrays;

import tech.dnaco.collections.arrays.ArrayUtil;
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

  private JsonEntityDataRows(final HashIndexedArray<String> fieldNames, final EntityDataType[] types, final Object[] values) {
    this.fieldNames = fieldNames;
    this.types = types;
    this.values = values;
  }

  public String[] getFieldNames() {
    return fieldNames.keySet();
  }

  public EntityDataType[] getTypes() {
    return types;
  }

  public boolean hasAllFields(final EntitySchema schema) {
    for (final String fieldName: fieldNames) {
      if (fieldName.startsWith("__")) continue;
      if (!schema.hasFieldName(fieldName)) {
        return false;
      }
    }
    return true;
  }

  public boolean forEachEntityRow(final EntitySchema schema, final String[] groups,
      final RowPredicate consumer) throws Exception {
    final int fieldCount = fieldNames.size();
    final EntityDataRows rows = new EntityDataRows(schema, hasAllFields(schema));
    for (int i = 0, n = rowCount(); i < n; ++i) {
      final int rowOffset = i * fieldCount;
      for (final String groupId: groups) {
        rows.reset().newRow();
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

  public static final class Builder {
    private static final int PAGE_SIZE = 5000;

    private final HashIndexedArray<String> fieldNames;
    private final EntityDataType[] types;
    private Object[] values;
    private int rowCount = 0;

    public Builder(final EntitySchema schema, String[] resultFields) {
      resultFields = fields(schema, resultFields);
      this.fieldNames = new HashIndexedArray<>(resultFields);
      this.types = new EntityDataType[resultFields.length];
      for (int i = 0; i < resultFields.length; ++i) {
        this.types[i] = schema.getFieldType(resultFields[i]);
      }
      this.values = new Object[0];
    }

    private static String[] fields(final EntitySchema schema, final String[] fieldNames) {
      final String[] resultFields;
      if (ArrayUtil.isEmpty(fieldNames)) {
        // TODO: remove system fields e.g. __group__, __seqid__
        resultFields = schema.getFieldNames().toArray(new String[0]);
      } else {
        resultFields = fieldNames;
      }
      Arrays.sort(resultFields);
      return resultFields;
    }

    public boolean isEmpty() {
      return rowCount == 0;
    }

    public void add(final EntityDataRow row) {
      final String[] fields = fieldNames.keySet();
      final int rowOffset = rowCount * fields.length;
      if (this.values.length == rowOffset) {
        this.values = Arrays.copyOf(values, rowOffset + (PAGE_SIZE * fields.length));
      }
      for (int i = 0; i < fields.length; ++i) {
        values[rowOffset + i] = row.getObject(fields[i]);
      }
      rowCount++;
    }

    public JsonEntityDataRows build() {
      final int nItems = rowCount * fieldNames.size();

      // get rows and reset values/rowCount
      final Object[] rows;
      if (nItems == values.length) {
        rows = values;
        values = new Object[0];
      } else {
        rows = Arrays.copyOf(values, nItems);
        Arrays.fill(values, null);
      }
      rowCount = 0;

      return new JsonEntityDataRows(fieldNames, types, rows);
    }
  }
}
