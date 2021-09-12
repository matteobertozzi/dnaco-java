package tech.dnaco.storage.net.models;

public class SchemaRequest {
  private String tenantId;
  private String name;

  public SchemaRequest() {
    // no-op
  }

  public String getTenantId() {
    return tenantId;
  }
  public void setTenantId(final String tenantId) {
    this.tenantId = tenantId;
  }
  public String getName() {
    return name;
  }
  public void setEntity(final String name) {
    this.name = name;
  }
}
