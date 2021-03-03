package tech.dnaco.storage.net.models;

import java.util.Arrays;

import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.storage.demo.EntityDataRow;
import tech.dnaco.storage.demo.EntitySchema;

public class ScanResult {
  public static final ScanResult EMPTY_RESULT = new ScanResult();

  private final JsonEntityDataRows rows;
  private String[] key;
  private String entity;
  private boolean more = false;

  private ScanResult() {
    this.rows = null;
    this.more = false;
  }

  public ScanResult(final EntitySchema schema) {
    this(schema, null);
  }

  public ScanResult(final EntitySchema schema, final String[] fields) {
    final String[] resultFields;
    if (ArrayUtil.isEmpty(fields)) {
      // TODO: remove system fields e.g. __group__, __seqid__
      resultFields = schema.getFieldNames().toArray(new String[0]);
    } else {
      resultFields = fields;
    }
    Arrays.sort(resultFields);

    this.rows = new JsonEntityDataRows(schema, resultFields);
    this.key = schema.getKeyFields();
    this.entity = schema.getEntityName();
  }

  public void add(final EntityDataRow row) {
    rows.add(row);
  }

  public JsonEntityDataRows getRows() {
    return rows;
  }

  public String[] getKey() {
    return key;
  }

  public void setKey(final String[] key) {
    this.key = key;
  }

  public String getEntity() {
    return entity;
  }

  public void setEntity(final String entity) {
    this.entity = entity;
  }

  public boolean isMore() {
    return more;
  }

  public void setMore(final boolean more) {
    this.more = more;
  }
}
