package tech.dnaco.storage.net.models;

public class TransactionCommitRequest {
  private String tenantId;
  private String txnId;
  private boolean rollback;

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
  public boolean isRollback() {
    return rollback;
  }
  public void setRollback(final boolean rollback) {
    this.rollback = rollback;
  }
}
