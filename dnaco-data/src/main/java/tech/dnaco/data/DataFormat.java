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

package tech.dnaco.data;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.strings.StringUtil;

public abstract class DataFormat {
  protected DataFormat() {
    // no-op
  }

  protected abstract DataFormatMapper<? extends JsonFactory> get();

  protected ObjectMapper getObjectMapper() {
    return get().getObjectMapper();
  }

  // ===============================================================================================
  //  JsonNode conversions
  // ===============================================================================================
  public JsonNode toTreeNode(final Object value) {
    return getObjectMapper().valueToTree(value);
  }

  public <T> T fromTreeNode(final JsonNode node, final Class<T> valueType) throws JsonProcessingException, IllegalArgumentException {
    return getObjectMapper().treeToValue(node, valueType);
  }

  // ===============================================================================================
  //  Object to Object conversions
  // ===============================================================================================
  public <T> T convert(final Object value, final Class<T> valueType) {
    return getObjectMapper().convertValue(value, valueType);
  }

  // ===============================================================================================
  //  From file/stream/byte[]/string/... conversions
  // ===============================================================================================
  public <T> T fromFile(final File file, final Class<T> valueType) throws IOException {
    try (FileInputStream stream = new FileInputStream(file)) {
      return fromStream(stream, valueType);
    }
  }

  public <T> T fromFile(final File file, final TypeReference<T> valueType) throws IOException {
    try (FileInputStream stream = new FileInputStream(file)) {
      return fromStream(stream, valueType);
    }
  }

  public <T> T fromStream(final InputStream stream, final Class<T> valueType) throws IOException {
    return getObjectMapper().readValue(stream, valueType);
  }

  public <T> T fromStream(final InputStream stream, final TypeReference<T> valueType) throws IOException {
    return getObjectMapper().readValue(stream, valueType);
  }

  public <T> T fromBytes(final byte[] data, final Class<T> valueType) {
    if (BytesUtil.isEmpty(data)) return null;
    try {
      return getObjectMapper().readValue(data, valueType);
    } catch (final Exception e) {
      throw new DataFormatException(e);
    }
  }

  public <T> T fromString(final String data, final Class<T> valueType) {
    if (StringUtil.isEmpty(data)) return null;
    try {
      return getObjectMapper().readValue(data, valueType);
    } catch (final JsonProcessingException e) {
      throw new DataFormatException(e);
    }
  }

  public <T> T fromString(final String data, final TypeReference<T> valueType) throws JsonProcessingException {
    if (StringUtil.isEmpty(data)) return null;
    try {
      return getObjectMapper().readValue(data, valueType);
    } catch (final JsonProcessingException e) {
      throw new DataFormatException(e);
    }
  }

  // ===============================================================================================
  //  To file/stream/byte[]/string/... conversions
  // ===============================================================================================
  public void addToStream(final OutputStream stream, final Object obj) throws IOException {
    getObjectMapper().writeValue(stream, obj);
  }

  public void addToPrettyPrintStream(final OutputStream stream, final Object obj) throws IOException {
    getObjectMapper().writerWithDefaultPrettyPrinter().writeValue(stream, obj);
  }

  public String asPrettyPrintString(final Object value) {
    try {
      return getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(value);
    } catch (final Exception e) {
      throw new DataFormatException(e);
    }
  }

  public String asString(final Object value) {
    try {
      return getObjectMapper().writeValueAsString(value);
    } catch (final Exception e) {
      throw new DataFormatException(e);
    }
  }

  public byte[] asBytes(final Object value) throws JsonProcessingException {
    return getObjectMapper().writeValueAsBytes(value);
  }

  public static final class DataFormatException extends RuntimeException {
	  private static final long serialVersionUID = -2079742671114280731L;

    public DataFormatException(final String msg) {
      super(msg);
    }

    public DataFormatException(final String msg, final Throwable cause) {
      super(msg, cause);
    }

    public DataFormatException(final Throwable cause) {
      super(cause);
    }
  }
}
