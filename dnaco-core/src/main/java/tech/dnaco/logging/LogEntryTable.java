package tech.dnaco.logging;

import tech.dnaco.collections.arrays.paged.PagedByteArray;
import tech.dnaco.logging.format.LogFormat;

public class LogEntryTable extends LogEntry {
  private String classAndMethod;
  private String label;
  private String[] columnNames;
  private String[] rows;

  @Override
  public LogEntryType getType() {
    return LogEntryType.TABLE;
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

  public String[] getColumnNames() {
    return columnNames;
  }

  public void setColumnNames(final String[] columnNames) {
    this.columnNames = columnNames;
  }

  public String[] getRows() {
    return rows;
  }

  public void setRows(final String[] rows) {
    this.rows = rows;
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
    entry.addTable(columnNames, rows);
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
