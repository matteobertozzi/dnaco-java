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

import java.util.Map;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.encoding.IntDecoder;
import tech.dnaco.bytes.encoding.VarInt;
import tech.dnaco.collections.LongValue;
import tech.dnaco.storage.format.FieldFormat;
import tech.dnaco.storage.format.FieldFormatReader;
import tech.dnaco.storage.format.SchemaFormat;

public class EntityRowsReader {
  private final Schema schema;

  private FieldFormatReader reader;
  private ByteArraySlice block;
  private int nextOffset;

  public EntityRowsReader(final Schema schema) {
    this.schema = schema;
  }

  public static EntityRowsReader newReader(final ByteArraySlice block) {
    final int schemaLength = Math.toIntExact(IntDecoder.LITTLE_ENDIAN.readFixed(block, block.length() - 3, 3));
    final int schemaOffset = block.length() - 3 - schemaLength;

    final LongValue rowCount = new LongValue();
    int offset = schemaOffset;
    //System.out.println("R:SCHEMA OFFSET: " + offset);
    offset += VarInt.read(block, offset, rowCount);  // row-count
    final Schema schema = SchemaFormat.readCompact(block, offset);  // schema

    final EntityRowsReader reader = new EntityRowsReader(schema);
    reader.load(new ByteArraySlice(block.rawBuffer(), block.offset(), schemaOffset));
    return reader;
  }

  public void load(final ByteArraySlice block) {
    final int version = block.get(0);
    this.reader = FieldFormat.newReader(schema, version);
    this.block = block;
    this.nextOffset = 1;
  }

  public Schema schema() {
    return schema;
  }

  public boolean hasMore() {
    //System.out.println("HAS MORE " + nextOffset + "/" + block.length());
    return nextOffset < block.length();
  }

  public FieldFormatReader next() {
    final int offset = nextOffset;
    nextOffset = Math.toIntExact(IntDecoder.LITTLE_ENDIAN.readFixed(block, offset, 4));
    final ByteArraySlice row = new ByteArraySlice(block.rawBuffer(), block.offset() + offset + 4, nextOffset - offset - 4);
    reader.load(row);
    return reader;
  }

  public void next(final Map<String, Object> row, final boolean includeNulls) {
    final FieldFormatReader reader = next();

    final int[] fieldIds = schema.fieldIds();
    for (int i = 0; i < fieldIds.length; ++i) {
      final int fieldId = fieldIds[i];
      final Object fieldValue = reader.get(schema.getFieldType(i), fieldId);
      if (includeNulls || fieldValue != null) {
        final String fieldName = schema.getFieldName(i);
        row.put(fieldName, fieldValue);
      }
    }
  }
}
