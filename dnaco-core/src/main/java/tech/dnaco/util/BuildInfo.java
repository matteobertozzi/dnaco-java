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

package tech.dnaco.util;

import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Enumeration;
import java.util.TimeZone;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import tech.dnaco.logging.Logger;
import tech.dnaco.strings.StringUtil;

public class BuildInfo {
  private static final String LOCAL_DATE_FORMAT = "yyyyMMddHHmmss";
  private static final String MVN_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";

  private static final String UNKNOWN = "unknown";

  private final String name;
  private String version = UNKNOWN;
  private String buildDate = UNKNOWN;
  private String createdBy = UNKNOWN;
  private String gitBranch = UNKNOWN;
  private String gitHash = UNKNOWN;

  public BuildInfo(final String name) {
    this.name = name;
    if (StringUtil.isEmpty(name)) {
      throw new IllegalArgumentException("expected name to be not empty");
    }
  }

  public String getName() {
    return name;
  }

  public String getVersion() {
    return version;
  }

  public String getBuildDate() {
    return buildDate;
  }

  public String getCreatedBy() {
    return createdBy;
  }

  public String getGitBranch() {
    return gitBranch;
  }

  public String getGitHash() {
    return gitHash;
  }

  public static BuildInfo loadInfoFromManifest(final String name) throws IOException, ParseException {
    final BuildInfo buildInfo = new BuildInfo(name);
    buildInfo.loadInfoFromManifest();
    return buildInfo;
  }

  public void loadInfoFromManifest() throws IOException, ParseException {
    final DateTimeFormatter localDateFormatter = DateTimeFormatter.ofPattern(LOCAL_DATE_FORMAT);
    final SimpleDateFormat mvnDateFormatter = new SimpleDateFormat(MVN_DATE_FORMAT);
    mvnDateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));

    final Enumeration<URL> resources = BuildInfo.class.getClassLoader().getResources("META-INF/MANIFEST.MF");
    while (resources.hasMoreElements()) {
      final Manifest manifest = new Manifest(resources.nextElement().openStream());
      final Attributes attributes = manifest.getMainAttributes();
      if (!StringUtil.equals(attributes.getValue("Implementation-Title"), name)) {
        continue;
      }

      // parse build timestamp
      final Instant utcInstant = mvnDateFormatter.parse(attributes.getValue("buildTimestamp")).toInstant();
      final ZonedDateTime utcBuildDate = ZonedDateTime.ofInstant(utcInstant, ZoneId.of("UTC"));
      final LocalDateTime localBuildDate = utcBuildDate.withZoneSameInstant(ZoneId.systemDefault()).toLocalDateTime();

      // add service info
      this.buildDate = localBuildDate.format(localDateFormatter);
      this.version = attributes.getValue("Implementation-Version");
      this.createdBy = StringUtil.defaultIfEmpty(attributes.getValue("Built-By"), attributes.getValue("builtBy"));
      this.gitBranch = attributes.getValue("gitBranch");
      this.gitHash = attributes.getValue("gitHash");

      Logger.trace("Loading manifest:\n -> entries: {}\n -> attributes: {}",
        manifest.getEntries(), manifest.getMainAttributes().entrySet());
      Logger.trace("{} version {} build-date {} created-by {}", name, getVersion(), localBuildDate, getCreatedBy());
      break;
    }
  }

  public boolean isValid() {
    return !StringUtil.equals(buildDate, UNKNOWN) && !StringUtil.equals(version, UNKNOWN);
  }

  @Override
  public String toString() {
    return "BuildInfo [name=" + name + ", version=" + version +
        ", buildDate=" + buildDate + ", createdBy=" + createdBy +
        ", gitBranch=" + gitBranch + ", gitHash=" + gitHash + "]";
  }
}
