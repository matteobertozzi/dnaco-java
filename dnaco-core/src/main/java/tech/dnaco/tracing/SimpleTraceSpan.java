package tech.dnaco.tracing;

import java.util.Arrays;

import tech.dnaco.data.JsonFormatUtil.JsonObject;
import tech.dnaco.logging.LogUtil;
import tech.dnaco.strings.StringUtil;
import tech.dnaco.util.JsonUtil;

public abstract class SimpleTraceSpan implements TraceSpan {
  private final Thread thread;
  private final long parentId;
  private final long traceId;
  private final long startTime;

  private final String callerMethodAndLine;
  private final String label;

  private String[] customKeys;
  private Object[] customVals;
  private long elapsedNs;

  protected SimpleTraceSpan(final Thread thread, final long parentId, final long traceId,
      final String callerMethodAndLine, final String label) {
    this.thread = thread;
    this.parentId = parentId;
    this.traceId = traceId;
    this.startTime = System.currentTimeMillis();

    this.callerMethodAndLine = callerMethodAndLine;
    this.label = label;

    this.customKeys = null;
    this.customVals = null;
    this.elapsedNs = System.nanoTime();
  }

  @Override
  public void close() {
    this.elapsedNs = System.nanoTime() - this.elapsedNs;
  }

  @Override
  public long getParentId() {
    return parentId;
  }

  @Override
  public long getTraceId() {
    return traceId;
  }

  public String getCallerMethodAndLine() {
    return callerMethodAndLine;
  }

  public String getLabel() {
    return label;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getElapsedNs() {
    return this.elapsedNs;
  }

  public long getElapsedNs(final long now) {
    return now - this.elapsedNs;
  }

  public Thread getThread() {
    return thread;
  }

  public String[] getCustomKeys() {
    return customKeys;
  }

  public Object[] getCustomVals() {
    return customVals;
  }

  @Override
  public void addData(final String key, final Object value) {
    if (this.customKeys != null) {
      customKeys = Arrays.copyOf(customKeys, 1 + customKeys.length);
      customVals = Arrays.copyOf(customVals, 1 + customVals.length);

      customKeys[customKeys.length - 1] = key;
      customVals[customVals.length - 1] = value;
    } else {
      customKeys = new String[] { key };
      customVals = new Object[] { value };
    }
  }

  public String getStringData(final String key) {
    return (String) getData(key);
  }

  public long getLongData(final String key, final long defaultValue) {
    final Object value = getData(key);
    return value != null ? (Long)value : defaultValue;
  }

  public Object getData(final String key) {
    if (customKeys == null) return null;
    for (int i = 0; i < customKeys.length; ++i) {
      if (StringUtil.equals(customKeys[i], key)) {
        return customVals[i];
      }
    }
    return null;
  }

  public JsonObject toJson() {
    final JsonObject json = new JsonObject();
    json.addProperty("traceId", LogUtil.toTraceId(traceId));
    json.addProperty("startTime", startTime);
    json.addProperty("elapsedNs", elapsedNs);
    json.addProperty("thread", thread.getName());
    addToJson(json);

    if (customKeys != null) {
      final JsonObject data = new JsonObject();
      for (int i = 0; i < customKeys.length; ++i) {
        data.add(customKeys[i], JsonUtil.toJsonTree(customVals[i]));
      }
      json.add("data", data);
    }
    return json;
  }

  protected abstract void addToJson(final JsonObject json);
}