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

package tech.dnaco.storage.service.storage;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPOutputStream;

import tech.dnaco.logging.Logger;
import tech.dnaco.util.ThreadUtil;

public final class Storage {
  public static final Storage INSTANCE = new Storage();

  private final AtomicBoolean running = new AtomicBoolean(false);
  private WalWriter walWriter;
  private Thread walThread;

  private Storage() {
    // no-op
  }

  public void start() {
    if (!running.compareAndSet(false, true)) {
      Logger.warn("service already started");
      return;
    }

    walWriter = new WalWriter(running);
    walThread = new Thread(walWriter, WalWriter.class.getSimpleName());
    walThread.start();
  }

  public void stop() {
    if (!running.compareAndSet(false, true)) {
      Logger.warn("service already stopped");
      return;
    }

    ThreadUtil.shutdown(walThread);
    walThread = null;
    walWriter = null;
  }

  private static final WalEntry POISON_WAL_ENTRY = new WalEntry();
  public static final class WalEntry {

  }

  private static final class WalWriter implements Runnable {
    private final LinkedTransferQueue<WalEntry> queue = new LinkedTransferQueue<>();
    private final AtomicBoolean running;

    private WalWriter(final AtomicBoolean running) {
      this.running = running;
    }

    @Override
    public void run() {
      while (running.get()) {
        WalEntry entry = waitAndFetchWalEntry();
        if (entry == null) continue;

        final File walFile = new File("wal.do");
        try (OutputStream stream = new GZIPOutputStream(new FileOutputStream(walFile, true))) {
          do {
            // TODO
          } while ((entry = fetchWalEntry()) != null);
          stream.flush();
        } catch (Exception e) {
          Logger.warn(e, "failed to write to the wal");
        } finally {

        }
      }
    }

    private WalEntry waitAndFetchWalEntry() {
      try {
        final WalEntry entry = queue.take();
        return (entry != null && entry != POISON_WAL_ENTRY) ? entry : null;
      } catch (InterruptedException e) {
        return null;
      }
    }

    private WalEntry fetchWalEntry() {
      final WalEntry entry = queue.poll();
      return (entry != null && entry != POISON_WAL_ENTRY) ? entry : null;
    }
  }
}
