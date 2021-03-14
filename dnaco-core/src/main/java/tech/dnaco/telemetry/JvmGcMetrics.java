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

package tech.dnaco.telemetry;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.openmbean.CompositeData;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;

import tech.dnaco.strings.HumansUtil;

public class JvmGcMetrics {
  public static final JvmGcMetrics INSTANCE = new JvmGcMetrics();

  private final MaxAndAvgTimeRangeGauge gcCurrentPhaseDuration = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_TIME_MILLIS)
    .setName("jvm.gc.current.phase.duration")
    .setLabel("JVM GC Current Phase Duration")
    .register(new MaxAndAvgTimeRangeGauge(60 * 24, 1, TimeUnit.MINUTES));

  private final TimeRangeCounter gcCurrentPhase = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_TIME_MILLIS)
    .setName("jvm.gc.current.phase.time")
    .setLabel("JVM Time spent in concurrent phase")
    .register(new TimeRangeCounter(60 * 24, 1, TimeUnit.MINUTES));

  private final MaxAndAvgTimeRangeGauge gcPauseDuration = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_TIME_MILLIS)
    .setName("jvm.gc.pause.duration")
    .setLabel("JVM GC Pause Duration")
    .register(new MaxAndAvgTimeRangeGauge(60 * 24, 1, TimeUnit.MINUTES));

  private final TimeRangeCounter gcPause = new TelemetryCollector.Builder()
    .setUnit(HumansUtil.HUMAN_TIME_MILLIS)
    .setName("jvm.gc.pause")
    .setLabel("JVM Time spent in GC pause")
    .register(new TimeRangeCounter(60 * 24, 1, TimeUnit.MINUTES));

  private JvmGcMetrics() {
    registerListeners();
  }

  public void collect(final long now) {
    // no-op
  }

  private static void onGcNotification(final Notification notification, final Object ref) {
    final GarbageCollectionNotificationInfo gcInfo = GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData());
    ((JvmGcMetrics)ref).onGcNotification(gcInfo);
  }

  private void onGcNotification(final GarbageCollectionNotificationInfo notification) {
    final GcInfo gcInfo = notification.getGcInfo();
    synchronized (INSTANCE) {
      final long now = System.currentTimeMillis();
      if (isConcurrentPhase(notification.getGcCause(), notification.getGcName())) {
        gcCurrentPhaseDuration.set(now, gcInfo.getDuration());
        gcCurrentPhase.add(now, gcInfo.getDuration());
      } else {
        gcPauseDuration.set(now, gcInfo.getDuration());
        gcPause.add(now, gcInfo.getDuration());
      }
    }
  }

  static boolean isConcurrentPhase(final String cause, final String name) {
    return "No GC".equals(cause) || "Shenandoah Cycles".equals(name);
  }

  private void registerListeners() {
    for (final GarbageCollectorMXBean mbean : ManagementFactory.getGarbageCollectorMXBeans()) {
      if (!(mbean instanceof NotificationEmitter)) {
        continue;
      }

      final NotificationEmitter notificationEmitter = (NotificationEmitter) mbean;
      notificationEmitter.addNotificationListener(JvmGcMetrics::onGcNotification, notification -> notification.getType().equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION), this);
    }
  }

  private static String toCollectorName(final String name) {
    final StringBuilder normalizedName = new StringBuilder(name.length());
    boolean lastWasSpace = false;
    for (int i = 0, n = name.length(); i < n; ++i) {
      final char c = name.charAt(i);
      if (Character.isLetterOrDigit(c)) {
        normalizedName.append(Character.toLowerCase(c));
        lastWasSpace = false;
      } else if (!lastWasSpace) {
        normalizedName.append('_');
        lastWasSpace = true;
      }
    }
    if (lastWasSpace) {
      normalizedName.setLength(normalizedName.length() - 1);
    }
    return normalizedName.toString();
  }
}
