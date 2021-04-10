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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import tech.dnaco.logging.Logger;
import tech.dnaco.strings.StringUtil;

public final class LinuxMemInfo {
  private long memTotal;
  private long memFree;
  private long buffer;
  private long cached;
  private long swapTotal;
  private long swapFree;

  public long getMemTotal() {
    return memTotal;
  }

  public long getMemFree() {
    return memFree;
  }

  public long getMemUsed() {
    return memTotal - memFree;
  }

  public long getBuffer() {
    return buffer;
  }

  public long getCached() {
    return cached;
  }

  public long getSwapTotal() {
    return swapTotal;
  }

  public long getSwapFree() {
    return swapFree;
  }

  public long getSwapUsed() {
    return swapTotal - swapFree;
  }

  public static boolean isSupported() {
    return new File("/proc/meminfo").exists();
  }

  // https://access.redhat.com/solutions/406773
  public static LinuxMemInfo readProcMemInfo() {
    try (BufferedReader reader = new BufferedReader(new FileReader("/proc/meminfo"))) {
      final LinuxMemInfo memInfo = new LinuxMemInfo();
      String line;
      while ((line = reader.readLine()) != null) {
        final int keyEndIndex = line.indexOf(':');
        if (keyEndIndex <= 0) continue;

        switch (line.substring(0, keyEndIndex)) {
          case "MemTotal":  memInfo.memTotal = readValue(line, keyEndIndex); break;
          case "MemFree":   memInfo.memFree = readValue(line, keyEndIndex); break;
          case "Buffers":   memInfo.buffer = readValue(line, keyEndIndex); break;
          case "Cached":    memInfo.cached = readValue(line, keyEndIndex); break;
          case "SwapTotal": memInfo.swapTotal = readValue(line, keyEndIndex); break;
          case "SwapFree":  memInfo.swapFree = readValue(line, keyEndIndex); break;
        }
      }
      return memInfo;
    } catch (final FileNotFoundException e) {
      // ignore: not a linux
    } catch (final IOException e) {
      Logger.error(e, "unable to read linux /proc/meminfo");
    }
    return null;
  }

  private static long readValue(final String line, final int offset) {
    // read value
    int vStart = offset;
    while (!Character.isDigit(line.charAt(vStart))) vStart++;
    int vEnd = vStart;
    while (vEnd < line.length() && Character.isDigit(line.charAt(vEnd))) vEnd++;
    final long v = Long.parseLong(line.substring(vStart, vEnd));

    // read suffix
    final String suffix = StringUtil.toLower(StringUtil.trim(line.substring(vEnd)));
    switch (suffix) {
      case "kb": return v * (1L << 10);
      case "mb": return v * (1L << 20);
      case "gb": return v * (1L << 30);
      case "tb": return v * (1L << 40);
    }
    return v;
  }
}
