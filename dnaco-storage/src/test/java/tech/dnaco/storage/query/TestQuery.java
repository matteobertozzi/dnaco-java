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

package tech.dnaco.storage.query;

import org.junit.jupiter.api.Test;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.encoding.RowKey;
import tech.dnaco.collections.arrays.paged.PagedByteArray;
import tech.dnaco.storage.DataTypes.DataType;
import tech.dnaco.storage.Schema;
import tech.dnaco.storage.format.FieldFormat;
import tech.dnaco.storage.format.FieldFormatReader;
import tech.dnaco.storage.format.FieldFormatWriter;
import tech.dnaco.storage.query.Filter.FilterType;

public class TestQuery {
  @Test
  public void testQuery() {
    final Schema schema = new Schema();
    schema.addField("bool", DataType.BOOL);
    schema.addField("int", DataType.INT);
    schema.addField("float", DataType.FLOAT);
    schema.addField("string", DataType.STRING);
    schema.setKeys(new String[] { "string", "int" });

    final Filter filter = Filter.newOrFilterBuilder()
      .add(Filter.newAndFilterBuilder()
        .eq("string", "bar")
        .lt("float", 20)
        .ne("bool", false)
        .build())
      .add(Filter.newAndFilterBuilder()
        .eq("string", "foo")
        .gt("float", 10)
        .ne("bool", false)
        .in("int", 1L, 2L, 3L)
        .build())
      .add(Filter.newAndFilterBuilder()
        .eq("string", "foo")
        .gt("float", 10)
        .ne("bool", false)
        .in("int", 1L, 2L, 3L)
        .build())
      .add(Filter.newAndFilterBuilder()
        .gt("int", 5)
        .lt("int", 10)
        .build())
      .add(new Filter(FilterType.EQ, "int", 5))
      .build();

    final PagedByteArray buffer = new PagedByteArray(1 << 10);
    try (FieldFormatWriter writer = FieldFormat.newWriter(schema, buffer, 0)) {
      writer.writeBool(schema.fieldIdByName("bool"), true);
      //writer.writeInt(schema.fieldIdByName("int"), 2);
      writer.writeFloat(schema.fieldIdByName("float"), 15.6);
      //writer.writeString(schema.fieldIdByName("string"), "foo");
    }
    final byte[] key = RowKey.newKeyBuilder().add("foo").addInt(2).drain();

    final FieldFormatReader reader = FieldFormat.newReader(schema, 0);
    reader.load(new ByteArraySlice(buffer.toByteArray()));

    final Query query = new Query(schema, filter);
    System.out.println("FILTER: " + filter.toQueryString());
    System.out.println("QUERY: " + query);
    final boolean result = query.process(key, reader);
    System.out.println("RESULT: " + result);
  }
}
