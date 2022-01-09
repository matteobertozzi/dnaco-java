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

package tech.dnaco.storage.format;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.collections.arrays.paged.PagedByteArray;
import tech.dnaco.storage.Schema;
import tech.dnaco.storage.format.v0.SchemaReaderV0;
import tech.dnaco.storage.format.v0.SchemaWriterV0;

public final class SchemaFormat {
  private SchemaFormat() {
    // no-op
  }

  private static final SchemaWriter[] SCHEMA_WRITERS = new SchemaWriter[] {
    new SchemaWriterV0()
  };

  private static final SchemaReader[] SCHEMA_READERS = new SchemaReader[] {
    new SchemaReaderV0()
  };

  public static void writeCompact(final Schema schema, final PagedByteArray buffer) {
    buffer.add(SCHEMA_WRITERS.length - 1);
    SCHEMA_WRITERS[SCHEMA_WRITERS.length - 1].writeCompactSchema(schema, buffer);
  }

  public static Schema readCompact(final ByteArraySlice block, int offset) {
    final int version = block.get(offset++);
    return SCHEMA_READERS[version].readCompactSchema(block, offset);
  }

  public interface SchemaReader {
    Schema readCompactSchema(final ByteArraySlice block, final int offset);
    Schema readSchema(final ByteArraySlice block, int offset);
  }

  public interface SchemaWriter {
    void writeCompactSchema(Schema schema, PagedByteArray buffer);
    void writeSchema(Schema schema, PagedByteArray buffer);
  }
}
