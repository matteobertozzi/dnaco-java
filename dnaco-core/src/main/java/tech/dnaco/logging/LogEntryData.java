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

package tech.dnaco.logging;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.collections.arrays.paged.PagedByteArray;
import tech.dnaco.collections.lists.ListUtil;
import tech.dnaco.logging.format.LogFormat;
import tech.dnaco.strings.HumansTableView;

public class LogEntryData extends LogEntry {
  public enum DataType { BINARY, TEXT, JSON, KEY_VALUE, TABLE, HTTP }

  private final ArrayList<Data> dataEntries = new ArrayList<>();
  private String label;

  @Override
  public LogEntryType getType() {
    return LogEntryType.DATA;
  }

  public String getLabel() {
    return label;
  }

  public LogEntryData setLabel(final String label) {
    this.label = label;
    return this;
  }

  public LogEntryData addData(final Data data) {
    this.dataEntries.add(data);
    return this;
  }

  public LogEntryData addBinary(final byte[] value) {
    return addData(new Binary(value));
  }

  public LogEntryData addText(final String value) {
    return addData(new Text(value));
  }

  public LogEntryData addJson(final String value) {
    return addData(new Json(value));
  }

  public LogEntryData addKeyValues(final Object[] kvs) {
    return addData(new KeyValue(kvs));
  }

  public LogEntryData addTable(final String[] columnNames, final Object[] rows) {
    return addData(new TableData(columnNames, rows));
  }

  public LogEntryData addHttpRequest(final String method, final String uri,
      final List<Entry<String, String>> headers, final byte[] body) {
    return addData(new HttpData(method, uri, headers, body));
  }

  public LogEntryData addHttpResponse(final String method, final String uri, final int status,
      final List<Entry<String, String>> headers, final byte[] body) {
    return addData(new HttpData(method, uri, status, headers, body));
  }

  public interface Data {
    DataType getType();
    void addToHumanReport(StringBuilder report);
  }

  public static final class Binary implements Data {
    private final byte[] value;

    public Binary(final byte[] value) {
      this.value = value;
    }

    @Override public DataType getType() { return DataType.BINARY; }

    public void addToHumanReport(final StringBuilder report) {
      for (int i = 0; i < value.length; ++i) {
        report.append(Integer.toHexString(value[i]));
        if ((i + 1) % 80 == 0) report.append("\n");
      }
    }
  }

  public static final class Text implements Data {
    private final String value;

    public Text(final String value) {
      this.value = value;
    }

    public DataType getType() { return DataType.TEXT; }

    public void addToHumanReport(final StringBuilder report) {
      report.append(value).append("\n");
    }
  }

  public static final class Json implements Data {
    private final String value;

    public Json(final String value) {
      this.value = value;
    }

    public DataType getType() { return DataType.JSON; }

    public void addToHumanReport(final StringBuilder report) {
      report.append(value).append("\n");
    }
  }

  public static final class KeyValue implements Data {
    private final Object[] kvs;

    public KeyValue(final Object[] kvs) {
      this.kvs = kvs;
    }

    public DataType getType() { return DataType.KEY_VALUE; }

    public void addToHumanReport(final StringBuilder report) {
      final HumansTableView tableView = new HumansTableView();
      tableView.addColumns("key", "value");
      for (int i = 0; i < kvs.length; i += 2) {
        if (kvs[i] != null) {
          tableView.addRow(kvs[i], kvs[i + 1]);
        } else {
          tableView.addSeparator();
        }
      }
      tableView.addHumanView(report, false);
    }
  }

  public static final class TableData implements Data {
    private final String[] columnNames;
    private final Object[] values;

    public TableData(final String[] columnNames, final Object[] values) {
      this.columnNames = columnNames;
      this.values = values;
    }

    public DataType getType() { return DataType.TABLE; }

    public void addToHumanReport(final StringBuilder report) {
      final HumansTableView tableView = new HumansTableView();
      tableView.addColumns(columnNames);
      for (int i = 0; i < values.length; i += columnNames.length) {
        tableView.addRow(values, i);
      }
      tableView.addHumanView(report);
    }
  }

  public static final class HttpData implements Data {
    private final String method;
    private final String uri;
    private final String[] headers;
    private final byte[] body;
    private final Integer status;

    public HttpData(final String method, final String uri,
        final List<Entry<String, String>> headers, final byte[] body) {
      this.method = method;
      this.uri = uri;
      this.headers = convertHeaders(headers);
      this.body = body;
      this.status = null;
    }

    public HttpData(final String method, final String uri, final int status,
        final List<Entry<String, String>> headers, final byte[] body) {
      this.method = method;
      this.uri = uri;
      this.headers = convertHeaders(headers);
      this.body = body;
      this.status = status;
    }

    private static String[] convertHeaders(final List<Entry<String, String>> entries) {
      if (ListUtil.isEmpty(entries)) return null;

      int index = 0;
      final String[] headers = new String[entries.size() * 2];
      for (final Entry<String, String> entry: entries) {
        headers[index++] = entry.getKey();
        headers[index++] = entry.getValue();
      }
      return headers;
    }

    public DataType getType() { return DataType.HTTP; }

    public void addToHumanReport(final StringBuilder report) {
      if (status != null) {
        report.append("RESPONSE STATUS: ").append(status).append("\n");
      }
      if (headers != null) {
        for (int i = 0; i < headers.length; i += 2) {
          report.append(" - ").append(headers[i]).append(" ").append(headers[i+1]).append("\n");
        }
      }
      if (BytesUtil.isNotEmpty(body)) {
        report.append(new String(body));
      }
      report.append("\n");
    }
  }

  public StringBuilder humanReport(final StringBuilder report) {
    super.humanReport(report);
    report.append(" - ").append(label).append("\n");
    for (final Data data: this.dataEntries) {
      data.addToHumanReport(report);
    }
    return report;
  }

  @Override
  public void writeData(final PagedByteArray buffer) {
    LogFormat.CURRENT.writeEntryData(buffer, this);
  }
}
