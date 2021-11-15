package tech.dnaco.logging;

import tech.dnaco.collections.arrays.paged.PagedByteArray;
import tech.dnaco.logging.format.LogFormat;

public class LogEntryJson extends LogEntry {
  private String classAndMethod;
  private String label;
  private String value;

  @Override
  public LogEntryType getType() {
    return LogEntryType.JSON;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(final String label) {
    this.label = label;
  }

  public String getClassAndMethod() {
    return classAndMethod;
  }

  public void setClassAndMethod(final String classAndMethod) {
    this.classAndMethod = classAndMethod;
  }

  public String getValue() {
    return value;
  }

  public void setValue(final String value) {
    this.value = value;
  }

  @Override
  protected void writeData(final PagedByteArray buffer) {
    LogFormat.CURRENT.writeEntryData(buffer, getDataEntry());
  }

  public StringBuilder humanReport(final StringBuilder report) {
    return getDataEntry().humanReport(report);
  }

  private LogEntryData getDataEntry() {
    final LogEntryData entry = new LogEntryData();
    entry.setLabel(label);
    entry.addJson(value);
    entry.setThread(getThread());
    entry.setTenantId(getGroupId());
    entry.setModule(getModule());
    entry.setOwner(getOwner());
    entry.setTraceId(getTraceId());
    entry.setSpanId(getSpanId());
    entry.setSeqId(getSeqId());
    entry.setTimestamp(getTimestamp());
    return entry;
  }
}
