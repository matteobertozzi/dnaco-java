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

package tech.dnaco.logging;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import tech.dnaco.collections.paged.PagedByteArray;
import tech.dnaco.journal.JournalEntry;

public abstract class LogEntry implements JournalEntry {
  public static final DateTimeFormatter LOG_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

  public enum LogEntryType { FLUSH, RESET, MESSAGE, TASK, DATA }

  private String thread;
  private String tenantId;
  private String module;
  private String owner;

  private long timestamp;
  private long traceId;

  public StringBuilder toHumanReport(final StringBuilder report) {
    final ZonedDateTime zdt = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault());
    report.append(LOG_DATE_FORMAT.format(zdt));
    report.append(" [").append(LogUtil.toTraceId(traceId)).append(":").append(thread);
    report.append(":").append(tenantId).append(":").append(module).append(":").append(owner).append("]");
    return report;
  }

  @Override
  public void release() {
    this.tenantId = null;
    this.module = null;
    this.owner = null;

    this.timestamp = 0;
    this.traceId = 0;
  }

  @Override
  public void write(final PagedByteArray buffer) {
    LogFormat.writeJournalHeader(buffer, this);
    final int offset = buffer.size();
    buffer.addFixed32(0);
    writeData(buffer);
    buffer.setFixed32(offset, buffer.size() - (offset + 4));
  }

  protected abstract void writeData(PagedByteArray buffer);

  public abstract LogEntryType getType();

  public String getThread() {
    return thread;
  }

  public void setThread(final String thread) {
    this.thread = thread;
  }

  @Override
  public String getTenantId() {
    return tenantId;
  }

  public void setTenantId(final String tenantId) {
    this.tenantId = tenantId;
  }

  public String getModule() {
    return module;
  }

  public void setModule(final String module) {
    this.module = module;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(final String owner) {
    this.owner = owner;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(final long timestamp) {
    this.timestamp = timestamp;
  }

  public long getTraceId() {
    return traceId;
  }

  public void setTraceId(final long traceId) {
    this.traceId = traceId;
  }
}
