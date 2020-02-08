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

public class LoggerSession {
  public static final String SYSTEM_PROJECT_ID = "__SYS__";

  private final String projectId;
  private final String moduleId;
  private final String groupId;
  private final LogLevel level;
  private final long traceId;

  protected LoggerSession(final String projectId, final String moduleId, final String groupId,
      final LogLevel level, final long traceId) {
    this.projectId = projectId;
    this.moduleId = StringUtil.nullIfEmpty(moduleId);
    this.groupId = StringUtil.nullIfEmpty(groupId);
    this.level = level;
    this.traceId = traceId;
  }

  public static LoggerSession newSession(final String projectId, final String moduleId,
      final String groupId, final LogLevel level, final long traceId) {
    return new LoggerSession(projectId, moduleId, groupId, level, traceId);
  }

  public static LoggerSession newSystemSession(final String moduleId, final String groupId,
      final LogLevel level, final long traceId) {
    return newSession(SYSTEM_PROJECT_ID, moduleId, groupId, level, traceId);
  }

  public static LoggerSession newSystemGeneralSession() {
    return newSession(SYSTEM_PROJECT_ID, "general", null, Logger.getDefaultLevel(), 0);
  }

  public String getProjectId() {
    return projectId;
  }

  public String getGroupId() {
    return groupId;
  }

  public String getModuleId() {
    return moduleId;
  }

  public LogLevel getLevel() {
    return level;
  }

  public long getTraceId() {
    return traceId;
  }

  @Override
  public String toString() {
    return "LoggerSession [projectId=" + projectId + ", groupId=" + groupId + ", moduleId=" + moduleId +
      ", level=" + level + ", traceId=" + traceId + "]";
  }
}
