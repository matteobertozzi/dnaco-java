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

package tech.dnaco.strings;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public final class HumansUtil {
  private HumansUtil() {
    // no-op
  }

  // ================================================================================
  //  Size related
  // ================================================================================
  public static String humanSize(final long size) {
    if (size >= (1L << 40)) return String.format("%.2fTiB", (float) size / (1L << 40));
    if (size >= (1L << 30)) return String.format("%.2fGiB", (float) size / (1L << 30));
    if (size >= (1L << 20)) return String.format("%.2fMiB", (float) size / (1L << 20));
    if (size >= (1L << 10)) return String.format("%.2fKiB", (float) size / (1L << 10));
    return size > 0 ? size + "bytes" : "0";
  }

  public static String humanCount(final long size) {
    if (size >= 1000000) return String.format("%.2fM", (float) size / 1000000);
    if (size >= 1000) return String.format("%.2fK", (float) size / 1000);
    return Long.toString(size);
  }

  // ================================================================================
  //  Rate related
  // ================================================================================
  public static String humanRate(final double rate) {
    if (rate >= 1000000000000.0) return String.format("%.2fT/sec", rate / 1000000000000.0);
    if (rate >= 1000000000.0) return String.format("%.2fG/sec", rate / 1000000000.0);
    if (rate >= 1000000.0) return String.format("%.2fM/sec", rate / 1000000.0);
    if (rate >= 1000.0) return String.format("%.2fK/sec", rate / 1000.0f);
    return String.format("%.2f/sec", rate);
  }

  // ================================================================================
  //  Date related
  // ================================================================================
  public static String humanDate(final long millis) {
    final SimpleDateFormat dfrmt = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
    return dfrmt.format(new Date(millis));
  }

  // ================================================================================
  //  Time related
  // ================================================================================
  public static String humanTimeSince(final long timeNano) {
    return humanTime(System.nanoTime() - timeNano, TimeUnit.NANOSECONDS);
  }

  public static String humanTimeNanos(final long timeDiff) {
    return humanTime(timeDiff, TimeUnit.NANOSECONDS);
  }

  public static String humanTimeMillis(final long timeDiff) {
    return humanTime(timeDiff, TimeUnit.MILLISECONDS);
  }

  public static String humanTime(final long timeDiff, final TimeUnit unit) {
    final long msec = unit.toMillis(timeDiff);
    if (msec == 0) {
      final long micros = unit.toMicros(timeDiff);
      if (micros > 0) return String.format("%dus", micros);
      return String.format("%dns", unit.toNanos(timeDiff));
    }

    if (msec < 1000) {
      return String.format("%dms", msec);
    }

    final long hours = msec / (60 * 60 * 1000);
    long rem = (msec % (60 * 60 * 1000));
    final long minutes = rem / (60 * 1000);
    rem = rem % (60 * 1000);
    final float seconds = rem / 1000.0f;

    if ((hours > 0) || (minutes > 0)) {
      final StringBuilder buf = new StringBuilder(32);
      if (hours > 0) {
        buf.append(hours);
        buf.append("hrs, ");
      }
      if (minutes > 0) {
        buf.append(minutes);
        buf.append("min, ");
      }

      final String humanTime;
      if (seconds > 0) {
        buf.append(String.format("%.2fsec", seconds));
        humanTime = buf.toString();
      } else {
        humanTime = buf.substring(0, buf.length() - 2);
      }

      if (hours > 24) {
        return String.format("%s (%.1f days)", humanTime, (hours / 24.0));
      }
      return humanTime;
    }

    return String.format((seconds % 1) != 0 ? "%.4fsec" : "%.0fsec", seconds);
  }

  // ================================================================================
  //  Millis util
  // ================================================================================
  public static ZonedDateTime localFromEpochMillis(final long localMillis) {
    return fromEpochMillis(localMillis, ZoneId.systemDefault());
  }

  public static ZonedDateTime utcFromEpochMillis(final long utcMillis) {
    return fromEpochMillis(utcMillis, ZoneOffset.UTC);
  }

  public static ZonedDateTime fromEpochMillis(final long millis, final ZoneId zoneId) {
    return Instant.ofEpochMilli(millis).atZone(zoneId);
  }

  public static long toEpochMillis(final ZonedDateTime dateTime) {
    return dateTime.toInstant().toEpochMilli();
  }

  // ================================================================================
  // Converters
  // ================================================================================
  public static final HumanLongValueConverter HUMAN_TIME_MILLIS = new HumanTimeConverter(TimeUnit.MILLISECONDS);
  public static final HumanLongValueConverter HUMAN_TIME_NANOS = new HumanTimeConverter(TimeUnit.NANOSECONDS);
  public static final HumanLongValueConverter HUMAN_COUNT = new HumanCountConverter();
  public static final HumanLongValueConverter HUMAN_SIZE = new HumanSizeConverter();
  public static final HumanLongValueConverter HUMAN_RATE = new HumanRateConverter();

  public interface HumanLongValueConverter {
    String getHumanType();
    String toHuman(long value);
  }

  public static final class HumanSizeConverter implements HumanLongValueConverter {
    @Override
    public String toHuman(final long value) {
      return HumansUtil.humanSize(value);
    }

    @Override
    public String getHumanType() {
      return "size";
    }
  }

  public static final class HumanCountConverter implements HumanLongValueConverter {
    @Override
    public String toHuman(final long value) {
      return HumansUtil.humanCount(value);
    }

    @Override
    public String getHumanType() {
      return "count";
    }
  }

  public static final class HumanRateConverter implements HumanLongValueConverter {
    @Override
    public String toHuman(final long value) {
      return HumansUtil.humanRate(value);
    }

    @Override
    public String getHumanType() {
      return "rate";
    }
  }

  public static final class HumanTimeConverter implements HumanLongValueConverter {
    private final TimeUnit unit;

    public HumanTimeConverter(final TimeUnit unit) {
      this.unit = unit;
    }

    @Override
    public String toHuman(final long value) {
      return HumansUtil.humanTime(value, unit);
    }

    @Override
    public String getHumanType() {
      switch (unit) {
        case DAYS: return "time-day";
        case HOURS: return "time-hour";
        case MICROSECONDS: return "time-us";
        case MILLISECONDS: return "time-ms";
        case MINUTES: return "time-min";
        case NANOSECONDS: return "time-ns";
        case SECONDS: return "time-sec";
      }
      throw new UnsupportedOperationException(unit.name());
    }
  }
}