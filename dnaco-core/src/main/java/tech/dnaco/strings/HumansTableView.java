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

package tech.dnaco.strings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HumansTableView {
  private static final int COLUMN_WRAP_LENGTH = 80;

  private ArrayList<String> columns = new ArrayList<>();
  private ArrayList<String[]> rows = new ArrayList<>();

  public HumansTableView addColumn(final String name) {
    this.columns.add(name);
    return this;
  }

  public HumansTableView addColumns(final List<String> names) {
    this.columns.addAll(names);
    return this;
  }
  
  public HumansTableView addRow(Object... rowValues) {
    return addRow(Arrays.asList(rowValues));
  }

  public <T> HumansTableView addRow(final List<T> rowValues) {
    final String[] row = new String[columns.size()];
    int index = 0;
    for (Object colValue: rowValues) {
      row[index++] = valueOf(colValue);
    }
    rows.add(row);
    return this;
  }

  private static String valueOf(final Object input) {
    if (input == null) return "(null)";

    final String value = String.valueOf(input);
    if (StringUtil.isEmpty(value)) return "";

    return value.replace('\t', ' ').replace('\n', ' ').replaceAll("\\s+", " ");
  }

  public StringBuilder addHumanView(final StringBuilder builder) {
    final int[] columnsLength = calcColumnsLength();

    drawHeaderBorder(builder, columnsLength);
    builder.append(drawRow(columnsLength, columns)).append('\n');
    drawHeaderBorder(builder, columnsLength);
    for (String[] row: this.rows) {
      builder.append(drawRow(columnsLength, Arrays.asList(row)));
      builder.append('\n');
    }
    drawHeaderBorder(builder, columnsLength);
    return builder;
  }

  private void drawHeaderBorder(final StringBuilder builder, final int[] columnsLength) {
    for (int i = 0; i < columnsLength.length; ++i) {
      builder.append("+-");
      for (int j = 0; j < columnsLength[i]; ++j) {
        builder.append('-');
      }
      builder.append('-');
    }
    builder.append("+\n");
  }

  private String drawRow(final int[] columnsLength, List<String> values) {
    final StringBuilder buf = new StringBuilder();
    final ArrayList<String> truncatedColumns = new ArrayList<>();
    boolean hasTruncation = false;
    for (int i = 0; i < columnsLength.length; ++i) {
      final String colValue = values.get(i);
      buf.append("| ");
      if (columnsLength[i] < colValue.length()) {
        truncatedColumns.add(colValue.substring(columnsLength[i]));
        buf.append(colValue.substring(0, columnsLength[i]));
        hasTruncation = true;
      } else {
        truncatedColumns.add("");
        buf.append(colValue);
        for (int k = 0, kN = (columnsLength[i] - colValue.length()); k < kN; ++k) {
          buf.append(' ');
        }
      }
      buf.append(' ');
    }
    buf.append('|');

    if (hasTruncation) {
      buf.append('\n');
      buf.append(drawRow(columnsLength, truncatedColumns));
    }
    return buf.toString();
  }

  private int[] calcColumnsLength() {
    final int[] columnsLength = new int[columns.size()];
    for (int i = 0, n = columnsLength.length; i < n; ++i) {
      columnsLength[i] = calcColumnLength(i);
    }
    return columnsLength;
  }

  private int calcColumnLength(final int index) {
    int length = columns.get(index).length();
    for (String[] row: this.rows) {
      length = Math.max(length, row[index].length());
    }
    return Math.min(length, COLUMN_WRAP_LENGTH);
  }

  public static void main(String[] args) {
    HumansTableView view = new HumansTableView();
    view.addColumns(Arrays.asList("foo", "bar"));
    view.addRow("car is in the bar", 10);
    view.addRow("zar is in the car", 20);
    view.addRow("war", 30);
    System.out.println(view.addHumanView(new StringBuilder()).toString());
  }
}
