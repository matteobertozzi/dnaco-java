/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package tech.dnaco.logging.format;

import tech.dnaco.collections.arrays.paged.PagedByteArray;
import tech.dnaco.journal.JournalAsyncWriter;
import tech.dnaco.journal.JournalGroupedBuffer;
import tech.dnaco.logging.LogEntry;
import tech.dnaco.logging.LoggingProvider;

public class LogFileProvider extends JournalAsyncWriter<LogEntry> implements LoggingProvider {
  public LogFileProvider() {
    super("logger", () -> new JournalGroupedBuffer<>(LogEntryWriter.INSTANCE));
  }

  public void start() {
    super.start(2500);
  }

  @Override
  public void addToLog(final Thread thread, final LogEntry entry) {
    addToLogQueue(thread, entry);
  }

  private static final class LogEntryWriter implements JournalEntryWriter<LogEntry> {
    private static final LogEntryWriter INSTANCE = new LogEntryWriter();

    @Override
    public void writeEntry(final PagedByteArray buffer, final LogEntry entry) {
      entry.write(buffer);
    }
  }
}
