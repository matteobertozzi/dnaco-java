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

package tech.dnaco.logging.format;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import tech.dnaco.logging.LogEntry;
import tech.dnaco.logging.LogEntry.LogEntryType;
import tech.dnaco.logging.Logger;
import tech.dnaco.logging.format.LogFormat.LogEntryHeader;
import tech.dnaco.logging.format.LogFormat.LogEntryReader;

public class LogReader {
  private final InputStream stream;
  private LogEntryReader reader;

  public LogReader(final InputStream stream) throws IOException {
    this.stream = stream;
    nextReader();
  }

  public LogEntryHeader getEntryHeader() {
    return reader.getEntryHeader();
  }

  public boolean readEntryHead() throws IOException {
    while (true) {
      try {
        // read type (1byte)
        final int vType = stream.read();
        if (vType < 0) throw new EOFException();

        //System.out.println(" -> TYPE: " + vType);
        final LogEntryType type = LogEntryType.values()[vType];
        //System.out.println(" -> TYPE: " + type);
        switch (type) {
          case FLUSH:
            if (!readFlushEntry()) {
              nextReader();
            }
            break;
          case RESET:
            reader.readResetEntry(stream);
            break;
          default:
            if (reader.fetchEntryHead(stream, type)) {
              return true;
            }
            nextReader();
            break;
        }
      } catch (final EOFException e) {
        throw e;
      } catch (final IOException e) {
        Logger.error("failed to read entries. skipping blocks: {}", e.getMessage());
        nextReader();
      }
    }
  }

  public LogEntry readEntryData() throws IOException {
    return reader.fetchEntryData(stream);
  }

  public void skipEntryData() throws IOException {
    reader.skipEntryData(stream);
  }

  private void nextReader() throws IOException {
    while (true) {
      final int type = stream.read();
      if (type < 0) throw new EOFException();
      if (type != LogEntryType.FLUSH.ordinal()) continue;
      //System.out.println("READ FLUSH TYPE " + type);
      if (readFlushEntry()) break;
    }
  }

  private boolean readFlushEntry() throws IOException {
    // FLUSH entry block start with a version field
    final int version = stream.read();
    if (version < 0) throw new EOFException();

    // invalid version
    //System.out.println("READ FLUSH VERSION " + version);
    if (version >= LogFormat.VERSIONS.length) return false;

    reader = LogFormat.VERSIONS[version].newEntryReader(stream);
    return reader != null;
  }
}
