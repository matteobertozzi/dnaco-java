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

import tech.dnaco.logging.LogUtil.LogLevel;
import tech.dnaco.strings.StringUtil;
import tech.dnaco.tracing.Span;
import tech.dnaco.tracing.Tracer;

public class LoggerSession {
  public static final String SYSTEM_PROJECT_ID = "__SYS__";

  private final String tenantId;
  private final String moduleId;
  private final String ownerId;
  private final LogLevel level;

  protected LoggerSession(final String tenantId, final String moduleId, final String ownerId, final LogLevel level) {
    this.tenantId = StringUtil.defaultIfEmpty(tenantId, "unknown");
    this.moduleId = StringUtil.defaultIfEmpty(moduleId, "unknown");
    this.ownerId = StringUtil.defaultIfEmpty(ownerId, "unknown");
    this.level = level;
  }

  public static LoggerSession newSession(final LogLevel level) {
    final Span span = Tracer.getCurrentTask();
    return newSession(span.getTenantId(), span.getModule(), null, level);
  }

  public static LoggerSession newSession(final String tenantId, final LoggerSession session) {
    return newSession(tenantId, session.getModuleId(), session.getOwnerId(), session.getLevel());
  }

  public static LoggerSession newSession(final String projectId, final String moduleId, final String ownerId) {
    return newSession(projectId, moduleId, ownerId, Logger.getDefaultLevel());
  }

  public static LoggerSession newSession(final String projectId, final String moduleId,
      final String ownerId, final LogLevel level) {
    return new LoggerSession(projectId, moduleId, ownerId, level);
  }

  public static LoggerSession newSystemSession(final String moduleId, final String ownerId) {
    return newSystemSession(moduleId, ownerId, Logger.getDefaultLevel());
  }

  public static LoggerSession newSystemSession(final String moduleId, final String ownerId, final LogLevel level) {
    return newSession(SYSTEM_PROJECT_ID, moduleId, ownerId, level);
  }

  public static LoggerSession newSystemGeneralSession() {
    return newSession(SYSTEM_PROJECT_ID, "general", "__SYS__", Logger.getDefaultLevel());
  }

  public String getTenantId() {
    return tenantId;
  }

  public String getModuleId() {
    return moduleId;
  }

  public String getOwnerId() {
    return ownerId;
  }

  public LogLevel getLevel() {
    return level;
  }

  @Override
  public String toString() {
    return "LoggerSession [projectId=" + tenantId + ", groupId=" + ownerId + ", moduleId=" + moduleId + ", level=" + level + "]";
  }
}
