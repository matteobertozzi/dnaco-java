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

import java.text.SimpleDateFormat;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;

import tech.dnaco.data.json.JsonElementModule;
import tech.dnaco.data.modules.DataMapperModules;
import tech.dnaco.data.modules.MapModule;
import tech.dnaco.data.modules.TraceIdsModule;
import tech.dnaco.util.Serialization.SerializationName;
import tech.dnaco.util.Serialization.SerializeWithSnakeCase;

public class DataFormatMapper {
  public static final String JSON_DATE_FORMAT_PATTERN = "YYYYMMddHHmmss";

  private final ObjectMapper mapper;

  protected DataFormatMapper(final ObjectMapper objectMapper) {
    this.mapper = objectMapper;
    this.mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
    this.mapper.setVisibility(PropertyAccessor.GETTER, Visibility.NONE);
    this.mapper.setVisibility(PropertyAccessor.IS_GETTER, Visibility.NONE);

    // --- Deserialization ---
    // Just ignore unknown fields, don't stop parsing
    this.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    // Trying to deserialize value into an enum, don't fail on unknown value, use null instead
    this.mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);

    // --- Serialization ---
    // Don't include properties with null value in JSON output
    this.mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    // Use default pretty printer
    this.mapper.configure(SerializationFeature.INDENT_OUTPUT, false);
    this.mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

    this.mapper.setDateFormat(new SimpleDateFormat(JSON_DATE_FORMAT_PATTERN));

    this.mapper.setAnnotationIntrospector(new ExtentedAnnotationIntrospector());

    // Default Modules
    registerModule(JsonElementModule.INSTANCE);
    registerModule(TraceIdsModule.INSTANCE);
    registerModule(MapModule.INSTANCE);
    for (final Module module: DataMapperModules.INSTANCE.getModules()) {
      registerModule(module);
    }
  }

  public void registerModule(final Module module) {
    this.mapper.registerModule(module);
  }

  public JsonFactory getFactory() {
    return mapper.getFactory();
  }

  public ObjectMapper getObjectMapper() {
    return mapper;
  }

  private static final class ExtentedAnnotationIntrospector extends JacksonAnnotationIntrospector {
    private static final long serialVersionUID = 1L;

    @Override
    public Object findNamingStrategy(final AnnotatedClass ac) {
      final SerializeWithSnakeCase ann = _findAnnotation(ac, SerializeWithSnakeCase.class);
      return (ann == null) ? super.findNamingStrategy(ac) : PropertyNamingStrategies.SNAKE_CASE;
    }

    @Override
    public PropertyName findNameForSerialization(final Annotated a) {
      final SerializationName ann = _findAnnotation(a, SerializationName.class);
      return ann == null ? super.findNameForSerialization(a) : PropertyName.construct(ann.value());
    }
  }
}
