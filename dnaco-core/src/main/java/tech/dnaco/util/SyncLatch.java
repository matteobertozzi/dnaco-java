package tech.dnaco.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

public class SyncLatch {
  private final Sync sync = new Sync();

  public void release() {
    sync.releaseShared(1);
  }

  public boolean await(final long timeout, final TimeUnit unit) throws InterruptedException {
    if (sync.tryAcquireSharedNanos(1, unit.toNanos(timeout))) {
      sync.reset();
      return true;
    }
    return false;
  }

  public void await() throws InterruptedException {
    sync.acquireSharedInterruptibly(1);
    sync.reset();
  }

  private static final class Sync extends AbstractQueuedSynchronizer {
    private static final long serialVersionUID = -3717842958437054687L;

	  private Sync() {
      setState(1);
    }

    private void reset() {
      setState(1);
    }

    protected int tryAcquireShared(int acquires) {
      return (getState() == 0) ? 1 : -1;
    }

    protected boolean tryReleaseShared(int releases) {
      // Decrement count; signal when transition to zero
      for (;;) {
        int c = getState();
        if (c == 0) return false;
        int nextc = c - 1;
        if (compareAndSetState(c, nextc))
          return nextc == 0;
      }
    }
  }
}