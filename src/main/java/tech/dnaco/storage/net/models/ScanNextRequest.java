package tech.dnaco.storage.net.models;

public class ScanNextRequest {
  private String tenantId;
  private String scannerId;
  public String getTenantId() {
    return tenantId;
  }
  public void setTenantId(final String tenantId) {
    this.tenantId = tenantId;
  }
  public String getScannerId() {
    return scannerId;
  }
  public void setScannerId(final String scannerId) {
    this.scannerId = scannerId;
  }
}
