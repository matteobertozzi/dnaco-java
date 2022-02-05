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
import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.Map.Entry;

import tech.dnaco.bytes.encoding.VarInt;
import tech.dnaco.collections.arrays.paged.PagedByteArray;
import tech.dnaco.data.json.JsonArray;
import tech.dnaco.data.json.JsonElement;
import tech.dnaco.data.json.JsonObject;
import tech.dnaco.data.json.JsonPrimitive;
import tech.dnaco.data.json.JsonUtil;
import tech.dnaco.storage.DataTypes.DataType;
import tech.dnaco.storage.format.FieldFormat;
import tech.dnaco.storage.format.FieldFormatReader;
import tech.dnaco.storage.format.FieldFormatWriter;
import tech.dnaco.storage.format.SchemaFormat;
import tech.dnaco.strings.HumansUtil;

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

  public static class SchemaBuilder {
    private final HashMap<String, DataType> fields = new HashMap<>();

    public void add(final String fieldName, final JsonElement value) {
      if (value == null || value.isJsonNull()) {
        //add(fieldName, DataType.NULL);
      } else if (value.isJsonPrimitive()) {
        final JsonPrimitive primitive = value.getAsJsonPrimitive();
        if (primitive.isNumber()) {
          final Number number = primitive.getAsNumber();
          if (number instanceof Long || number instanceof Integer) {
            add(fieldName, DataType.INT);
          } else if (number instanceof Double || number instanceof Float) {
            add(fieldName, DataType.FLOAT);
          } else {
            throw new UnsupportedOperationException("unsupported number type " + number.getClass() + " " + number);
          }
        } else if (primitive.isString()) {
          add(fieldName, DataType.STRING);
        } else if (primitive.isBoolean()) {
          add(fieldName, DataType.BOOL);
        } else {
          throw new UnsupportedOperationException("unsupported primitive type " + primitive.getClass() + " " + primitive);
        }
      } else if (value.isJsonObject()) {
        add(fieldName, DataType.OBJECT);
      } else if (value.isJsonArray()) {
        add(fieldName, DataType.ARRAY);
      } else {
        throw new UnsupportedOperationException("unsupported json element " + value.getClass() + " " + value);
      }
    }

    public void add(final String fieldName, final DataType dataType) {
      fields.put(fieldName, dataType);
    }

    @Override
    public int hashCode() {
      return fields.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof final SchemaBuilder other)) return false;

      return Objects.equals(fields, other.fields);
    }
  }

  public static void main(final String[] args) throws Exception {
    final JsonArray items = JsonUtil.fromJson(new File("data.json"), JsonArray.class);
    System.out.println(" -> " + items.size());

    final HashSet<SchemaBuilder> schemas = new HashSet<>();
    final long startTime = System.nanoTime();
    for (final JsonElement item: items) {
      final JsonObject json = item.getAsJsonObject();
      final SchemaBuilder schema = new SchemaBuilder();
      for (final Entry<String, JsonElement> entry: json.entrySet()) {
        schema.add(entry.getKey(), entry.getValue());
      }
      schemas.add(schema);
      //System.out.println(" --> " + schema.fields);
    }
    final long elapsed = System.nanoTime() - startTime;
    System.out.println(HumansUtil.humanTimeNanos(elapsed));
    System.out.println("schemas: " + schemas.size());
  }
}
