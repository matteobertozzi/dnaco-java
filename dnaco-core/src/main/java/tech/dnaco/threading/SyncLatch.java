/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.dnaco.threading;

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

    protected int tryAcquireShared(final int acquires) {
      return (getState() == 0) ? 1 : -1;
    }

    protected boolean tryReleaseShared(final int releases) {
      // Decrement count; signal when transition to zero
      for (;;) {
        final int c = getState();
        if (c == 0) return false;
        final int nextc = c - 1;
        if (compareAndSetState(c, nextc))
          return nextc == 0;
      }
    }
  }
}
