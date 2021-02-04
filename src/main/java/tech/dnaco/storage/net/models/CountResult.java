package tech.dnaco.storage.net.models;

public class CountResult {
  public static CountResult EMPTY = new CountResult(0);

  private long totalRows;

  public CountResult() {
    // no-op
  }

  public CountResult(final int totalRows) {
    this.totalRows = totalRows;
  }

  public long getTotalRows() {
    return totalRows;
  }

  public void setTotalRows(final long totalRows) {
    this.totalRows = totalRows;
  }

  public void incTotalRows() {
    this.totalRows++;
  }
}
