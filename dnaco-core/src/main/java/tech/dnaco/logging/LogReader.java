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

package tech.dnaco.logging;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.function.Predicate;
import java.util.zip.GZIPInputStream;

import tech.dnaco.collections.LongArrayList;
import tech.dnaco.io.IOUtil;
import tech.dnaco.logging.LogUtil.LogLevel;

public class LogReader implements AutoCloseable {
  private final LogEntry entry = new LogEntry();
  private final InputStream stream;

  public LogReader(final File logFile) throws IOException {
    this.stream = new GZIPInputStream(new FileInputStream(logFile));
  }

  public LogEntry read() throws IOException {
    try {
      return entry.readBinary(stream, null) ? entry : null;
    } catch (final EOFException e) {
      return null;
    }
  }

  public LogEntry read(final Predicate<LogEntry> headerPredicate) throws IOException {
    try {
      while (!entry.readBinary(stream, headerPredicate));
      return entry;
    } catch (final EOFException e) {
      return null;
    }
  }

  @Override
  public void close() {
    IOUtil.closeQuietly(stream);
  }

  public static void main(final String[] args) throws Exception {
    final LongArrayList filterTraceIds = new LongArrayList(8);
    final HashSet<LogLevel> filterLevels = new HashSet<>(8);

    for (int i = 0; i < args.length; ++i) {
      if (args[i].equals("-l")) {
        filterLevels.add(LogLevel.valueOf(args[i]));
      } else if (args[i].equals("-t")) {
        filterTraceIds.add(Long.parseLong(args[i]));
      }
    }

    final Predicate<LogEntry> headerPredicate = new Predicate<LogEntry>() {
      @Override
      public boolean test(final LogEntry entry) {
        if (filterTraceIds.isNotEmpty() && filterTraceIds.indexOf(0, entry.getTraceId()) < 0) {
          return false;
        }
        return filterLevels.isEmpty() || filterLevels.contains(entry.getLevel());
      }
    };

    final File logFile = new File("logs/general");
    try (LogReader reader = new LogReader(logFile)) {
      LogEntry entry;
      while ((entry = reader.read(headerPredicate)) != null) {
        entry.printEntry(null, System.out);
      }
    }
  }
}
