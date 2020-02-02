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

import tech.dnaco.logging.LogUtil.LogLevel;
import tech.dnaco.strings.StringUtil;

public final class LogTraceBuffer {
  private static ThreadLocal<LogTraceBufferData> traceBuffer = new ThreadLocal<>();

  private LogTraceBuffer() {
    // no-op
  }

  public static void add(final String projectId, final LogEntry entry) {
    traceBuffer.get().add(projectId, entry);
  }

  private static final class LogTraceBufferData {
    private static final int TRACES_MASK = 7;
    private static final int ENTRIES_MASK = 7;

    private final LogEntry[] entries = new LogEntry[1 + ENTRIES_MASK];
    private final String[] traces = new String[1 + TRACES_MASK];
    private String currentProjectId;
    private long nextEntries = 0;
    private long nextTraces = 0;

    public void add(final String projectId, final LogEntry entry) {
      if (!StringUtil.equals(currentProjectId, projectId)) {
        flushTraces(currentProjectId);
        this.currentProjectId = projectId;
      }

      final int index = Math.toIntExact(nextEntries++ & ENTRIES_MASK);
      this.entries[index] = entry;

      if (entry.getLevel().ordinal() <= LogLevel.ERROR.ordinal()) {
        flushTraces(projectId);
      }
    }

    public void flushTraces(final String projectId) {
      final int index = Math.toIntExact(nextTraces++ & TRACES_MASK);
      final int entriesCount = Math.toIntExact(Math.min(nextEntries, entries.length));
      final StringBuilder builder = new StringBuilder(1024);
      builder.append("--- ").append(projectId).append(" ---\n");
      for (int i = entriesCount; i > 0; --i) {
        entries[Math.toIntExact(nextEntries - i)].printEntry(null, builder);
      }
      builder.append('\n');
      traces[index] = builder.toString();
    }
  }
}