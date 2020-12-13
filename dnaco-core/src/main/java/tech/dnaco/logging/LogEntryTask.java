package tech.dnaco.logging;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import tech.dnaco.collections.ArrayUtil;
import tech.dnaco.collections.paged.PagedByteArray;
import tech.dnaco.strings.HumansUtil;

public class LogEntryTask extends LogEntry {
  private String[] customKeys;
  private String[] customVals;
  private String callerMethodAndLine;
  private String label;
  private long elapsedNs = -1;
  private long parentId;

  public long getParentId() {
    return parentId;
  }

  public void setParentId(final long parentId) {
    this.parentId = parentId;
  }

  public long getElapsedNs() {
    return elapsedNs;
  }

  public void setElapsedNs(final long elapsedNs) {
    this.elapsedNs = elapsedNs;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(final String label) {
    this.label = label;
  }

  public void setCallerMethodAndLine(final String callerMethodAndLine) {
    this.callerMethodAndLine = callerMethodAndLine;
  }

  public String getCallerMethodAndLine() {
    return callerMethodAndLine;
  }

  public int getAttributesCount() {
    return customKeys != null ? customKeys.length : 0;
  }

  public String getAttributeKey(final int index) {
    return customKeys[index];
  }

  public String getAttributeValue(final int index) {
    return customVals[index];
  }

  public void setAttributes(final String[] keys, final Object[] vals) {
    if (ArrayUtil.isNotEmpty(keys)) {
      this.customKeys = keys;
      this.customVals = new String[vals.length];
      for (int i = 0; i < vals.length; ++i) {
        this.customVals[i] = String.valueOf(vals[i]);
      }
    } else {
      this.customKeys = null;
      this.customVals = null;
    }
  }

  @Override
  public LogEntryType getType() {
    return LogEntryType.TASK;
  }

  @Override
  protected void writeData(final PagedByteArray buffer) {
    LogFormat.CURRENT.writeEntryTask(buffer, this);
  }

  public boolean isComplete() {
    return elapsedNs >= 0;
  }

  @Override
  public StringBuilder toHumanReport(final StringBuilder report) {
    final ZonedDateTime zdt = Instant.ofEpochMilli(getTimestamp()).atZone(ZoneId.systemDefault());
    report.append(LOG_DATE_FORMAT.format(zdt));
    report.append(" [").append(LogUtil.toTraceId(parentId)).append(":");
    report.append(LogUtil.toTraceId(getTraceId())).append(":").append(getThread());
    report.append(":").append(":").append(getModule()).append(":").append(getOwner()).append("]");

    report.append((elapsedNs < 0) ? " STARTED " : " COMPLETED ");
    if (elapsedNs >= 0) {
      report.append("in ").append(HumansUtil.humanTimeNanos(elapsedNs)).append(" ");
    }

    report.append(label).append(" - ").append(callerMethodAndLine);

    if (customKeys != null) {
      report.append(" - {");
      for (int i = 0; i < customKeys.length; ++i) {
        if (i > 0) report.append(", ");
        report.append(customKeys[i]).append(": ").append(customVals[i]);
      }
      report.append("}");
    }
    return report;
  }
}
