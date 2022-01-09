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

package tech.dnaco.storage;

import java.io.Closeable;

import tech.dnaco.bytes.encoding.VarInt;
import tech.dnaco.collections.arrays.paged.PagedByteArray;
import tech.dnaco.storage.format.FieldFormat;
import tech.dnaco.storage.format.FieldFormatReader;
import tech.dnaco.storage.format.FieldFormatWriter;
import tech.dnaco.storage.format.SchemaFormat;

public class EntityRowsWriter implements Closeable {
  private final FieldFormatWriter writer;
  private final PagedByteArray buffer;
  private final Schema schema;

  private int lastRowOffset;
  private int rowCount;

  public EntityRowsWriter(final Schema schema) {
    this(schema, 1 << 20);
  }

  public EntityRowsWriter(final Schema schema, final int bufPageSize) {
    this.buffer = new PagedByteArray(bufPageSize);
    this.schema = schema;

    this.writer = FieldFormat.newWriter(schema, buffer);
    this.buffer.add(writer.getVersion()); // format
  }

  @Override
  public void close() {
    final int schemaOffset = buffer.size();
    VarInt.write(buffer, rowCount);     // row-count
    SchemaFormat.writeCompact(schema, buffer);  // schema
    buffer.addFixed(3, buffer.size() - schemaOffset);   // offset to row-count
  }

  public int bufferSize() {
    return buffer.size();
  }

  public int rowCount() {
    return rowCount;
  }

  public byte[] toByteArray() {
    return buffer.toByteArray();
  }

  public FieldFormatWriter newRow() {
    lastRowOffset = buffer.size();
    buffer.addFixed32(0);   // next row offset
    writer.reset();
    return writer;
  }

  public void closeRow() {
    writer.close();
    buffer.setFixed32(lastRowOffset, buffer.size());
    rowCount++;
  }

  public void addRow(final byte[] rowValue) {
    buffer.addFixed32(buffer.size() + rowValue.length);   // next row offset
    buffer.add(rowValue);
    rowCount++;
  }

  public EntityRowConverter newRowConverter(final Schema rowSchema) {
    return new EntityRowConverter(rowSchema, schema);
  }

  public void addRow(final Schema rowSchema, final FieldFormatReader rowReader) {
    final EntityRowConverter converter = newRowConverter(rowSchema);
    addRow(converter, rowReader);
  }

  public void addRow(final EntityRowConverter converter, final FieldFormatReader rowReader) {
    final FieldFormatWriter writer = newRow();
    converter.convert(rowReader, writer);
    closeRow();
  }
}
