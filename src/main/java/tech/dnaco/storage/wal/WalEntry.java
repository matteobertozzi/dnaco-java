package tech.dnaco.storage.wal;

import tech.dnaco.collections.arrays.paged.PagedByteArray;
import tech.dnaco.journal.JournalEntry;

public class WalEntry implements JournalEntry {
  private String tenantId;
  private long seqId;

  @Override
  public void release() {
    // no-op
  }

  @Override
  public String getGroupId() {
    return tenantId;
  }

  public void setTenantId(final String value) {
    this.tenantId = value;
  }

  public long getSeqId() {
    return seqId;
  }

  public void setSeqId(final long seqId) {
    this.seqId = seqId;
  }

  public void write(final PagedByteArray buffer) {
    // TODO Auto-generated method stub
  }
}
