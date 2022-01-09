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

package tech.dnaco.data.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.tika.Tika;

import tech.dnaco.strings.StringUtil;

public final class MimeUtil {
  public static final MimeUtil INSTANCE = new MimeUtil();

  public static final String APPLICATION_OCTET_STREAM = "application/octet-stream";
  public static final String APPLICATION_JAVASCRIPT = "application/javascript";
  public static final String APPLICATION_JSON = "application/json";
  public static final String APPLICATION_PDF = "application/pdf";
  public static final String APPLICATION_XML = "application/xml";

  public static final String TEXT_PLAIN = "text/plain";
  public static final String TEXT_CSS = "text/css";
  public static final String TEXT_HTML = "text/html";
  public static final String TEXT_CSV = "text/csv";
  private static final String TEXT_XML = "text/xml";
  private static final String TEXT_JAVASCRIPT = "text/javascript";

  private static final String IMAGE_SVG_XML = "image/svg+xml";
  private static final String IMAGE_SVG = "image/svg";

  /*
   * The list is limited to the most common mime types known to be compressable. Compressing already
   * compressed content does not harm other than wasting some CPU cycles.
   */
  private static final HashSet<String> COMPRESSABLE = new HashSet<>(32);
  static {
    COMPRESSABLE.add(TEXT_PLAIN);
    COMPRESSABLE.add(TEXT_CSS);
    COMPRESSABLE.add(TEXT_HTML);
    COMPRESSABLE.add(TEXT_CSV);
    COMPRESSABLE.add(TEXT_JAVASCRIPT);
    COMPRESSABLE.add(TEXT_XML);

    COMPRESSABLE.add(APPLICATION_XML);
    COMPRESSABLE.add(APPLICATION_JAVASCRIPT);
    COMPRESSABLE.add(APPLICATION_JSON);

    COMPRESSABLE.add(IMAGE_SVG);
    COMPRESSABLE.add(IMAGE_SVG_XML);
  }

  private final Tika tika = new Tika();

  private MimeUtil() {
    // no-op
  }

  public String detectMimeType(final byte[] data) {
    return tika.detect(data);
  }

  public String detectMimeType(final String name, final byte[] data) {
    return tika.detect(data, name);
  }

  public String detectMimeType(final File file) throws IOException {
    return tika.detect(file);
  }

  /**
   * Determines if it is recommended to compress data of the given mime type.
   * <p>
   * It isn't very useful to compress JPEG or PNG files, as they are already compressed. This method
   * is very pessimistic: Only a set of known content types is accepted as compressable.
   * </p>
   * @param contentType the mime type to check
   * @return <tt>true</tt> if the mime type is recognized as compressable, <tt>false</tt> otherwise
   */
  public boolean isCompressable(String contentType) {
    if (StringUtil.isEmpty(contentType)) return false;

    final int idx = contentType.indexOf(';');
    if (idx >= 0) {
      contentType = contentType.substring(0, idx);
    }
    return StringUtil.isNotEmpty(contentType) && COMPRESSABLE.contains(contentType);
  }

  public static List<String> parseMimeTypes(final String mimeTypes) {
    if (mimeTypes.equals("*/*"));

    // text/html, application/xhtml+xml, application/xml;q=0.9, image/webp, */*;q=0.8
    final ArrayList<String> parsedTypes = new ArrayList<>();
    int offset = 0;
    while (offset < mimeTypes.length()) {
      int index = mimeTypes.indexOf(',', offset);
      if (index < 0) index = mimeTypes.length();

      final String type = mimeTypes.substring(offset, index);
      final int wIndex = type.indexOf(';');
      parsedTypes.add(((wIndex < 0) ? type : type.substring(0, wIndex)).trim());

      offset = index + 1;
    }
    return parsedTypes;
  }
}
