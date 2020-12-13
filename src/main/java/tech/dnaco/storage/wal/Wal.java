package tech.dnaco.storage.wal;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import tech.dnaco.journal.JournalAsyncWriter;

public final class Wal {
  public static final Wal INSTANCE = new Wal();

  private final AtomicLong seqId = new AtomicLong(0);
  private JournalAsyncWriter walWriter = null;

  private Wal() {
    // no-op
  }

  public void replay() throws IOException {

  }

  public void setWalWriter(final JournalAsyncWriter walWriter) {
    this.walWriter = walWriter;
  }

  public void append(final WalEntry entry) {
    entry.setSeqId(seqId.incrementAndGet());
    this.walWriter.addToLogQueue(Thread.currentThread(), entry);
  }
}
