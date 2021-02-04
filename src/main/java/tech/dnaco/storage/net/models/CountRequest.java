package tech.dnaco.storage.net.models;

import tech.dnaco.storage.demo.Filter;

public class CountRequest {
  private String tenantId;
  private String txnId;
  private String entity;
  private String[] groups;
  private Filter filter;

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
  public boolean hasNoFilter() {
    return filter == null;
  }
  public Filter getFilter() {
    return filter;
  }
  public void setFilter(final Filter filter) {
    this.filter = filter;
  }
}
