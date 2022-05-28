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

import com.fasterxml.jackson.dataformat.xml.XmlMapper;

public final class XmlFormat extends DataFormat {
  public static final XmlFormat INSTANCE = new XmlFormat();

  private static final ThreadLocal<XmlFormatMapper> mapper = ThreadLocal.withInitial(XmlFormatMapper::new);

  private XmlFormat() {
    // no-op
  }

  @Override
  public String name() {
    return "XML";
  }

  @Override
  public String contentType() {
    return "application/xml";
  }

  @Override
  protected DataFormatMapper get() {
    return mapper.get();
  }

  private static final class XmlFormatMapper extends DataFormatMapper {
    private XmlFormatMapper() {
      super(new XmlMapper());
    }
  }
}
