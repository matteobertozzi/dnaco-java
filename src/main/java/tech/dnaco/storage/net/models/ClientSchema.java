package tech.dnaco.storage.net.models;

import java.util.Set;

public class ClientSchema {
  private String tenantId;

  private String name;
  private String label;
  private String dataType;
  private long retentionPeriod;
  private long editTime;
  private long rowCount;
  private long diskUsage;
  private boolean sync;
  private EntityField[] attributes;
  private Set<String> groups;

  public ClientSchema() {
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

  public void setName(final String name) {
    this.name = name;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(final String label) {
    this.label = label;
  }

  public boolean getSync() {
    return sync;
  }

  public void setSync(final boolean sync) {
    this.sync = sync;
  }

  public String getDataType() {
    return dataType;
  }

  public void setDataType(final String dataType) {
    this.dataType = dataType;
  }

  public long getRetentionPeriod() {
    return retentionPeriod;
  }

  public void setRetentionPeriod(final long retentionPeriod) {
    this.retentionPeriod = retentionPeriod;
  }

  public EntityField[] getFields() {
    return attributes;
  }

  public void setFields(final EntityField[] fields) {
    this.attributes = fields;
  }

  public void setEditTime(final long time) {
    this.editTime = time;
  }

  public long getEditTime() {
    return editTime;
  }

  public void setRowCount(final long value) {
    this.rowCount = value;
  }

  public long getRowCount() {
    return rowCount;
  }

  public void setDiskUsage(final long value) {
    this.diskUsage = value;
  }

  public void setGroups(final Set<String> groups) {
    this.groups = groups;
  }

  public long getDiskUsage() {
    return diskUsage;
  }

  public static class EntityField {
    private String name;
    private boolean key;
    private String type;
    private long diskUsage;

    public EntityField() {
      // no-op
    }

    public String getName() {
      return name;
    }

    public void setName(final String name) {
      this.name = name;
    }
    public boolean isKey() {
      return key;
    }
    public void setKey(final boolean key) {
      this.key = key;
    }
    public String getType() {
      return type;
    }
    public void setType(final String type) {
      this.type = type;
    }
    public long getDiskUsage() {
      return diskUsage;
    }
    public void setDiskUsage(final long diskUsage) {
      this.diskUsage = diskUsage;
    }
  }
}
