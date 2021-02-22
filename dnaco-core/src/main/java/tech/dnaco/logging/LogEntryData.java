package tech.dnaco.logging;

import java.util.ArrayList;

import tech.dnaco.collections.paged.PagedByteArray;
import tech.dnaco.strings.HumansTableView;

public class LogEntryData extends LogEntry {
  private final ArrayList<Data> dataEntries = new ArrayList<>();
  private String dataType;
  private String label;

  public static void main(final String[] args) {
    final LogEntryData entry = new LogEntryData();
    entry.setDataType("FOO");
    entry.setLabel("label");
    entry.addKeyValues(new Object[] { "aaa", "vvv", "bbb", "vvv", null, null, "ccc", "vvv" });
    entry.addJson("{}");
    System.out.println(entry.toHumanReport(new StringBuilder()));
  }

	@Override
	public LogEntryType getType() {
		return LogEntryType.DATA;
  }

  public String getDataType() {
    return dataType;
  }

  public LogEntryData setDataType(final String type) {
    this.dataType = type;
    return this;
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

	@Override
	protected void writeData(final PagedByteArray buffer) {
		// TODO Auto-generated method stub

  }

  public StringBuilder toHumanReport(final StringBuilder report) {
    super.toHumanReport(report);
    report.append(" - ").append(dataType);
    report.append(" - ").append(label).append("\n");
    for (final Data data: this.dataEntries) {
      data.addToHumanReport(report);
    }
    return report;
  }

  public enum DataType { BINARY, TEXT, JSON, KEY_VALUE, TABLE }

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
}