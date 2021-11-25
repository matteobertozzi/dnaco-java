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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.function.ObjLongConsumer;
import java.util.function.Predicate;
import java.util.zip.GZIPInputStream;

import tech.dnaco.logging.LogEntry;
import tech.dnaco.logging.LogEntryMessage;
import tech.dnaco.logging.LogUtil.LogLevel;
import tech.dnaco.logging.Logger;
import tech.dnaco.logging.format.LogFormat.LogEntryHeader;
import tech.dnaco.threading.ThreadUtil;
import tech.dnaco.tracing.TraceId;

public class LogFileReader {
  private final File file;
  private final String tenantId;

  private long lineCount;
  private long offset;

  public LogFileReader(final File file) {
    this.file = file;
    this.tenantId = tenantIdFromFile(file.getName());
    this.offset = 0;
    this.lineCount = 0;
  }

  private static String tenantIdFromFile(final String name) {
    return name;
  }

  public void skipToEnd() throws IOException {
    read((header) -> false, (entry, lineNo) -> {});
  }

  public void read(final Predicate<LogEntryHeader> predicate, final ObjLongConsumer<LogEntry> consumer) throws IOException {
    try (FileInputStream fileStream = new FileInputStream(file)) {
      fileStream.getChannel().position(offset);
      if (fileStream.available() <= 0) return;

      try (GZIPInputStream stream = new GZIPInputStream(fileStream)) {
        final LogReader reader = new LogReader(stream);
        while (reader.readEntryHead()) {
          lineCount++;
          if (predicate.test(reader.getEntryHeader())) {
            final LogEntry entry = reader.readEntryData();
            if (entry != null) {
              entry.setTenantId(tenantId);
              consumer.accept(entry, lineCount);
            }
          } else {
            reader.skipEntryData();
          }
          offset = fileStream.getChannel().position();
        }
      } catch (final IOException e) {
        // no-op (EOFException)
      }
    } catch (final FileNotFoundException e) {
      // no-op (we are probably trying to tail something that will show up later)
    }
  }

  private static final class Params {
    private final ArrayList<LogFileReader> files = new ArrayList<>();
    private final Set<LogLevel> levels = new HashSet<>();
    private final Set<TraceId> traceIds = new HashSet<>();
    private final Set<String> threads = new HashSet<>();
    private final Set<String> modules = new HashSet<>();
    private final Set<String> owners = new HashSet<>();
    private boolean countLines;
    private boolean tail;

    public boolean matchFilter(final LogEntryHeader head) {
      if (!threads.isEmpty() && threads.contains(head.threadName)) return true;
      if (!modules.isEmpty() && threads.contains(head.module)) return true;
      if (!owners.isEmpty() && threads.contains(head.owner)) return true;
      if (!traceIds.isEmpty() && traceIds.contains(head.getTraceId())) return true;
      // if everything is empty math all
      return threads.isEmpty() && modules.isEmpty() && owners.isEmpty() && traceIds.isEmpty();
    }

    public boolean matchFilter(final LogEntry entry) {
      if (entry instanceof LogEntryMessage) {
        if (!levels.isEmpty() && levels.contains(((LogEntryMessage)entry).getLevel())) return true;
        // if everything is empty math all
        return levels.isEmpty();
      }
      return true;
    }

    public void parse(final String[] args) {
      for (int i = 0; i < args.length; ++i) {
        if (args[i].equals("-T")) {
          traceIds.add(TraceId.fromString(args[++i]));
        } else if (args[i].equals("-m")) {
          modules.add(args[++i]);
        } else if (args[i].equals("-o")) {
          owners.add(args[++i]);
        } else if (args[i].equals("-t")) {
          threads.add(args[++i]);
        } else if (args[i].equals("-l")) {
          levels.add(LogLevel.valueOf(args[++i]));
        } else if (args[i].equals("-f")) {
          tail = true;
        } else if (args[i].equals("-h")) {
          showHelp();
          System.exit(1);
        } else if (args[i].equals("-n")) {
          countLines = true;
        } else {
          files.add(new LogFileReader(new File(args[i])));
        }
      }
    }

    private void showHelp() {
      System.out.println("Usage:");
      System.out.println("  LogFileReader [options...] [files...]");
      System.out.println();
      System.out.println("  -f                wait and print new data once available");
      System.out.println("  -n                add line number, starting from 1");
      System.out.println("  -T <trace id>     grep logs with specified TraceId");
      System.out.println("  -m <module name>  grep logs with module name");
      System.out.println("  -o <owner name>   grep logs with owner name");
      System.out.println("  -t <thread name>  grep logs with thread name");
      System.out.println("  -l <level>        grep logs with specified level");
    }
  }

  public static void main(final String[] args) throws Exception {
    final Params params = new Params();
    params.parse(args);

    if (params.tail) {
      for (final LogFileReader reader: params.files) {
        if (!reader.file.exists()) {
          Logger.warn("file {} does not exists yet", reader.file);
        }
        reader.skipToEnd();
      }
    }
    do {
      final StringBuilder report = new StringBuilder(1024);
      for (final LogFileReader reader: params.files) {
        reader.read(params::matchFilter, (entry, lineNo) -> {
          if (!params.matchFilter(entry)) return;

          report.setLength(0);
          if (params.countLines) report.append(String.format("%8d\t", lineNo));
          System.out.println(entry.humanReport(report));
        });
      }
      ThreadUtil.sleep(500);
    } while (params.tail);
  }
}
