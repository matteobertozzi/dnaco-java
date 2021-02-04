package tech.dnaco.storage.net.models;

import tech.dnaco.collections.ArrayUtil;

public class ModificationRequest {
  private String tenantId;
  private String txnId;
  private String entity;
  private String[] groups;
  private String[] keys;
  private JsonEntityDataRows[] rows;

  public boolean hasData() {
    return ArrayUtil.isNotEmpty(rows);
  }

  public String getTenantId() {
    return tenantId;
  }

  public void setTenantId(final String tenantId) {
    this.tenantId = tenantId;
  }

  public String getTxnId() {
    return txnId;
  }

  public void setTxnId(final String txnId) {
    this.txnId = txnId;
  }

  public String getEntity() {
    return entity;
  }

  public void setEntity(final String entity) {
    this.entity = entity;
  }

  public String[] getGroups() {
    return groups;
  }

  public void setGroups(final String[] groups) {
    this.groups = groups;
  }

  public String[] getKeys() {
    return keys;
  }

  public void setKeys(final String[] keys) {
    this.keys = keys;
  }

  public JsonEntityDataRows[] getRows() {
    return rows;
  }

  public void setRows(final JsonEntityDataRows[] rows) {
    this.rows = rows;
  }


}
