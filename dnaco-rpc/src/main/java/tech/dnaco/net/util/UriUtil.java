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

package tech.dnaco.net.util;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import tech.dnaco.logging.Logger;
import tech.dnaco.strings.StringUtil;

public final class UriUtil {
  private static final Pattern INSECURE_URI = Pattern.compile(".*[<>&\"].*");

  private UriUtil() {
    // no-op
  }

  public static String join(final String prefix, final String path) {
    if (StringUtil.isEmpty(prefix)) return pathToUri(path);
    if (StringUtil.isEmpty(path)) return pathToUri(prefix);
    return path.startsWith(prefix) ? pathToUri(path) : pathToUri(new File(prefix, path).toString());
  }

  private static String pathToUri(final String path) {
    return StringUtil.replace(path, File.separatorChar, "/");
  }

  public static String join(final String... parts) {
    if (parts.length == 1) return parts[0];

    String baseUri = parts[0];
    for (int i = 1; i < parts.length; i++) {
      baseUri = join(baseUri, parts[i]);
    }
    return baseUri;
  }

  public static String sanitizeUri(final String uriPrefix, String uri) {
    // Decode the path.
    try {
      uri = URLDecoder.decode(uri.substring(uriPrefix.length() - 1), StandardCharsets.UTF_8.name());
    } catch (final UnsupportedEncodingException e) {
      Logger.debug("unsupported encoding {}", uri);
      throw new Error(e);
    }

    if (uri.isEmpty() || uri.charAt(0) != '/') {
      return null;
    }

    // Convert file separators.
    uri = uri.replace('/', File.separatorChar);

    // TODO: Improve, Simplistic dumb security check.
    if (uri.contains(File.separator + '.') ||
        uri.contains('.' + File.separator) ||
        uri.charAt(0) == '.' ||
        uri.charAt(uri.length() - 1) == '.' ||
        INSECURE_URI.matcher(uri).matches()) {
      return null;
    }

    // Convert to absolute path.
    return new File(uri).toString();
  }

  private static final Pattern PATH_PATTERN = Pattern.compile("^(/[-\\w:@&?=+,.!/~*'%$_;]*)?$");
  private static boolean isValidPath(final String path) {
    if (path == null) {
      return false;
    }

    if (!PATH_PATTERN.matcher(path).matches()) {
      return false;
    }

    final int slash2Count = countToken("//", path);
    if (slash2Count > 0) {
      return false;
    }

    final int slashCount = countToken("/", path);
    final int dot2Count = countToken("..", path);
    System.out.println(dot2Count + " -> " + ((slashCount - slash2Count - 1)));
    if (dot2Count > 0 && (slashCount - slash2Count - 1) <= dot2Count){
      return false;
    }
    return true;
  }

  private static int countToken(final String token, final String target) {
    int tokenIndex = 0;
    int count = 0;
    while (tokenIndex >= 0) {
      if ((tokenIndex = target.indexOf(token, tokenIndex)) < 0) {
        break;
      }
      tokenIndex++;
      count++;
    }
    return count;
  }

  public static String extractUriParam(final String url, final String paramName) {
    return extractUriParam(url, 0, paramName);
  }

  public static String extractUriParam(final String url, final int offset, final String paramName) {
    final String token = paramName + '=';
    final int idxUser = url.indexOf(token, offset);
    if (idxUser < 0) return null;

    final int startIdx = idxUser + token.length();
    int endIndex = url.indexOf('&', startIdx);
    if (endIndex <= 0) {
      endIndex = url.indexOf(';', startIdx);
      if (endIndex < 0) endIndex = url.length();
    }
    return url.substring(startIdx, endIndex);
  }

  public static String extractUriParam(final String url, final int offset, final String[] paramName) {
    for (int i = 0; i < paramName.length; ++i) {
      final String value = extractUriParam(url, offset, paramName[i]);
      if (!StringUtil.isEmpty(value)) return value;
    }
    return null;
  }
}
