package tech.dnaco.logging;

import tech.dnaco.collections.paged.PagedByteArray;

public class LogEntryTask extends LogEntry {
  private String[] customKeys;
  private Object[] customVals;
  private String label;
  private long elapsedNs;
  private long parentId;

  @Override
  protected void writeData(final PagedByteArray buffer) {
    // TODO Auto-generated method stub

  }

  @Override
  public LogEntryType getType() {
    // TODO Auto-generated method stub
    return null;
  }

}
