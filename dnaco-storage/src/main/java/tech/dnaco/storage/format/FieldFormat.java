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

import tech.dnaco.collections.arrays.paged.PagedByteArray;
import tech.dnaco.storage.Schema;
import tech.dnaco.storage.format.v0.FieldFormatReaderV0;
import tech.dnaco.storage.format.v0.FieldFormatWriterV0;

public final class FieldFormat {
  private FieldFormat() {
    // no-op
  }

  private static final FieldFormatWriterFactory[] WRITER_FACTORY = new FieldFormatWriterFactory[] {
    (buffer, schema) -> new FieldFormatWriterV0(buffer, schema.fieldsCount()),
  };

  private static final FieldFormatReaderFactory[] READER_FACTORY = new FieldFormatReaderFactory[] {
    schema -> new FieldFormatReaderV0(schema.fieldsCount()),
  };

  public static FieldFormatReader newReader(final Schema schema, final int version) {
    return READER_FACTORY[version].create(schema);
  }

  public static FieldFormatWriter newWriter(final Schema schema, final PagedByteArray buffer) {
    return newWriter(schema, buffer, WRITER_FACTORY.length - 1);
  }

  public static FieldFormatWriter newWriter(final Schema schema, final PagedByteArray buffer, final int version) {
    return WRITER_FACTORY[version].create(buffer, schema);
  }

  @FunctionalInterface
  interface FieldFormatWriterFactory {
    FieldFormatWriter create(PagedByteArray buffer, Schema schema);
  }

  @FunctionalInterface
  interface FieldFormatReaderFactory {
    FieldFormatReader create(Schema schema);
  }
}
