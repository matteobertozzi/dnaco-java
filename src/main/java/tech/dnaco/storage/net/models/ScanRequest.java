package tech.dnaco.storage.net.models;

import tech.dnaco.storage.demo.Filter;

public class ScanRequest {
  private String tenantId;
  private String txnId;
  private String entity;
  private String[] groups;
  private String[] fields;
  private JsonEntityDataRows[] rows;
  private Filter filter;
  private boolean includeDeleted;
  private boolean syncOnly;
  private int limit;

  public ScanRequest() {
    // no-op
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
  public String[] getFields() {
    return fields;
  }
  public void setFields(final String[] fields) {
    this.fields = fields;
  }
  public JsonEntityDataRows[] getRows() {
    return rows;
  }
  public void setRows(final JsonEntityDataRows[] rows) {
    this.rows = rows;
  }
  public boolean hasNoFilter() {
    return filter == null;
  }
  public Filter getFilter() {
    return filter;
  }
  public void setFilter(final Filter filter) {
    this.filter = filter;
  }
  public boolean shouldIncludeDeleted() {
    return includeDeleted;
  }
  public void shouldIncludeDeleted(final boolean value) {
    this.includeDeleted = value;
  }

  public boolean isSyncOnly() {
    return syncOnly;
  }

  public void setSyncOnly(final boolean syncOnly) {
    this.syncOnly = syncOnly;
  }

  public int getLimit() {
    return limit;
  }

  public void setLimit(final int value) {
    this.limit = value;
  }
}
