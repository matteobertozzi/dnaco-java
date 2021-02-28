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

import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.collections.arrays.paged.PagedByteArray;
import tech.dnaco.logging.LogUtil.LogLevel;
import tech.dnaco.logging.format.LogFormat;
import tech.dnaco.strings.StringFormat;
import tech.dnaco.strings.StringUtil;

public class LogEntryMessage extends LogEntry {
  private String exception;
  private String classAndMethod;
  private String msgFormat;
  private String[] msgArgs;
  private LogLevel level;

  @Override
  public LogEntryType getType() {
    return LogEntryType.MESSAGE;
  }

  public boolean hasException() {
    return StringUtil.isNotEmpty(exception);
  }

  public String getException() {
    return exception;
  }

  public void setException(final Throwable exception) {
    setException(exception != null ? LogUtil.stackTraceToString(exception) : null);
  }

  public void setException(final String exception) {
    this.exception = exception;
  }

  public String getClassAndMethod() {
    return classAndMethod;
  }

  public void setClassAndMethod(final String classAndMethod) {
    this.classAndMethod = classAndMethod;
  }

  public String getMsgFormat() {
    return msgFormat;
  }

  public void setMsgFormat(final String msgFormat) {
    this.msgFormat = msgFormat;
  }

  public boolean hasMsgArgs() {
    return msgArgs != null;
  }

  public String[] getMsgArgs() {
    return msgArgs;
  }

  public void setMsgArgs(final String[] msgArgs) {
    this.msgArgs = ArrayUtil.isNotEmpty(msgArgs) ? msgArgs : null;
  }

  public LogLevel getLevel() {
    return level;
  }

  public void setLevel(final LogLevel level) {
    this.level = level;
  }

  public StringBuilder humanReport(final StringBuilder report) {
    super.humanReport(report);
    report.append(" ").append(level).append(" ").append(classAndMethod).append(" - ");
    StringFormat.applyFormat(report, msgFormat, msgArgs);
    if (exception != null) report.append(" - ").append(exception);
    return report;
  }

  @Override
  public void writeData(final PagedByteArray buffer) {
    LogFormat.CURRENT.writeEntryMessage(buffer, this);
  }
}