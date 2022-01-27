package tech.dnaco.storage.net.models;

public class ScanResult {
  public static final ScanResult EMPTY_RESULT = new ScanResult();

  private JsonEntityDataRows rows;
  private String[] key;
  private String entity;
  private boolean more;

  public ScanResult() {
    // no-op
  }

  public ScanResult(final String entity, final String[] key, final boolean more, final JsonEntityDataRows rows) {
    this.rows = rows;
    this.key = key;
    this.entity = entity;
    this.more = more;
  }

  public JsonEntityDataRows getRows() {
    return rows;
  }

  public String[] getKey() {
    return key;
  }

  public void setKey(final String[] key) {
    this.key = key;
  }

  public String getEntity() {
    return entity;
  }

  public void setEntity(final String entity) {
    this.entity = entity;
  }

  public boolean isMore() {
    return more;
  }

  public void setMore(final boolean more) {
    this.more = more;
  }
}
