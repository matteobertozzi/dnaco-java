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

package tech.dnaco.tracing;

public final class TraceAttributes {
  public static final String HOST_ID = "host.id";
  public static final String HOST_NAME = "host.name";
  public static final String HOST_TYPE = "host.type";
  public static final String HOST_IMAGE_NAME = "host.image.name";
  public static final String HOST_IMAGE_ID = "host.image.id";
  public static final String HOST_IMAGE_VERSION = "host.image.version";

  public static final String CLOUD_PROVIDER = "cloud.provider";
  public static final String CLOUD_REGION = "cloud.region";
  public static final String CLOUD_ZONE = "cloud.zone";

  public static final String OS_ARCH = "os.arch";
  public static final String OS_NAME = "os.name";
  public static final String OS_VERSION = "os.version";

  public static final String PROCESS_PID = "process.pid";
  public static final String PROCESS_OWNER = "process.owner";
  public static final String PROCESS_RUNTIME_NAME = "process.runtime.name";
  public static final String PROCESS_RUNTIME_VERSION = "process.runtime.version";

  public static final String HTTP_METHOD = "http.method";
  public static final String HTTP_URI = "http.uri";
  public static final String HTTP_STATUS_CODE = "http.status_code";
  public static final String HTTP_CLIENT_IP = "http.client_ip";

  public static final String OWNER = "owner";
  public static final String MODULE = "module";
  public static final String QUEUE_TIME = "queue.time";
  public static final String THREAD_NAME = "thread.name";

  private TraceAttributes() {
    // no-op
  }
}
