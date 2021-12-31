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

package tech.dnaco.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.logging.Logger;
import tech.dnaco.strings.StringUtil;

public final class FileUtil {
  private static final File TEMP_DIR = new File(System.getProperty("java.io.tmpdir"));

  private FileUtil() {
    // no-op
  }

  // ===============================================================================================
  //  Temp Dir/Files related
  // ===============================================================================================
  public static File tempDir() {
    return TEMP_DIR;
  }

  public static File tempFile(final String name) {
    return new File(TEMP_DIR, name);
  }

  // ===============================================================================================
  //  File Name related
  // ===============================================================================================
  public static final Comparator<File> FILE_NAME_COMPARATOR = (a, b) -> StringUtil.compare(a.getName(), b.getName());
  public static final Comparator<File> FILE_NAME_REVERSED_COMPARATOR = (a, b) -> StringUtil.compare(b.getName(), a.getName());

  public static boolean nameEndsWith(final String fileName, final String... acceptedExt) {
    for (int i = 0; i < acceptedExt.length; ++i) {
      if (fileName.endsWith(acceptedExt[i])) return true;
    }
    return false;
  }

  public static String getExtension(final String name) {
    final int dotIndex = name.lastIndexOf('.');
    return name.substring(dotIndex + 1);
  }

  public static String stripExtension(final String name) {
    final int dotIndex = name.lastIndexOf('.');
    if (dotIndex < 0) return name;
    return name.substring(0, dotIndex);
  }

  // ===============================================================================================
  //  File Visitor related
  // ===============================================================================================
  public interface FileVisitor {
    boolean visit(File file);
  }

  public static boolean filesVisitor(final File rootDir, final FileVisitor visitor) {
    final File[] files = rootDir.listFiles();
    if (ArrayUtil.isEmpty(files)) {
      return true;
    }

    for (int i = 0; i < files.length; ++i) {
      if (files[i].isDirectory()) {
        filesVisitor(files[i ], visitor);
      } else if (!visitor.visit(files[i])) {
        return false;
      }
    }
    return true;
  }

  // ===============================================================================================
  //  Directories related
  // ===============================================================================================
  public static List<File> listAll(final File file) {
    if (!file.exists()) return Collections.emptyList();

    if (file.isDirectory()) {
      final List<File> files = new ArrayList<>();
      listAll(files, file);
      return files;
    }
    return Collections.singletonList(file);
  }

  private static void listAll(final List<File> allFiles, final File dir) {
    final File[] files = dir.listFiles();
    if (ArrayUtil.isEmpty(files)) return;

    for (int i = 0; i < files.length; ++i) {
      if (files[i].isDirectory()) {
        listAll(allFiles, files[i]);
      } else {
        allFiles.add(files[i]);
      }
    }
  }

  public static void recursiveDelete(final File file) {
    if (file.isDirectory()) {
      final File[] dirFiles = file.listFiles();
      if (ArrayUtil.isNotEmpty(dirFiles)) {
        for (int i = 0; i < dirFiles.length; ++i) {
          recursiveDelete(dirFiles[i]);
        }
      }
    }

    if (!file.delete() && file.exists()) {
      Logger.warn("unable to delete file {}", file);
    }
  }

  // ===============================================================================================
  //  GZIP Files
  // ===============================================================================================
  public static void createGzipFile(final File file, final byte[] data)
      throws IOException {
    createGzipFile(file, data, 0, BytesUtil.length(data));
  }

  public static void createGzipFile(final File file, final byte[] data, final int off, final int len)
      throws IOException {
    try (OutputStream stream = new GZIPOutputStream(new FileOutputStream(file))) {
      stream.write(data, off, len);
    }
  }

  public static byte[] readGzipFile(final File file) throws IOException {
    try (FileInputStream stream = new FileInputStream(file)) {
      try (GZIPInputStream gz = new GZIPInputStream(stream)) {
        return gz.readAllBytes();
      }
    }
  }
}
