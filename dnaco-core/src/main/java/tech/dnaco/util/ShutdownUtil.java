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

import java.util.concurrent.atomic.AtomicBoolean;

import tech.dnaco.logging.Logger;
import tech.dnaco.logging.LoggerSession;
import tech.dnaco.strings.HumansUtil;

public final class ShutdownUtil {
  private ShutdownUtil() {
    // no-op
  }

  public interface StopSignal {
    boolean sendStopSignal();
  }

  public static void addShutdownHook(final String name, final StopSignal... services) {
    addShutdownHook(name, Thread.currentThread(), services);
  }

  public static void addShutdownHook(final String name, final Thread mainThread, final StopSignal... services) {
    addShutdownHook(name, mainThread, new AtomicBoolean(true), services);
  }

  public static void addShutdownHook(final String name, final AtomicBoolean running, final StopSignal... services) {
    addShutdownHook(name, Thread.currentThread(), running, services);
  }

  public static void addShutdownHook(final String name, final Thread mainThread, final AtomicBoolean running, final StopSignal... services) {
    Runtime.getRuntime().addShutdownHook(new Thread(name + "ShutdownHook") {
      @Override
      public void run() {
        Logger.setSession(LoggerSession.newSystemGeneralSession());
        final long startTime = System.nanoTime();
        Logger.info("{} shutdown hook!", name);
        for (int i = 0; i < services.length; ++i) {
          Logger.debug("sending stop signal to: {}", services[i]);
          services[i].sendStopSignal();
        }
        running.set(false);
        Logger.debug("waiting for {} to finish", mainThread);
        ThreadUtil.shutdown(mainThread);
        Logger.info("shutdown took: {}", HumansUtil.humanTimeSince(startTime));
      }
    });
  }
}