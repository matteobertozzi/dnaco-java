package tech.dnaco.logging;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import tech.dnaco.collections.arrays.paged.PagedByteArray;

public class LogEntryHttp extends LogEntry {
  private String uri;
  private String[] headers;
  private String body;

  private String method;
  private Integer status;

  @Override
  public LogEntryType getType() {
    return LogEntryType.HTTP;
  }

  public String getUri() {
    return uri;
  }

  public void setUri(final String uri) {
    this.uri = uri;
  }

  public String[] getHeaders() {
    return headers;
  }

  public void setHeaders(final String[] headers) {
    this.headers = headers;
  }

  public void setHeaders(final Map<String, String> headers) {
    setHeaders(headers.entrySet());
  }

  public void setHeaders(final Collection<Map.Entry<String,String>> headers) {
    this.headers = new String[headers.size() * 2];
    int index = 0;
    for (final Entry<String, String> entry: headers) {
      this.headers[index++] = entry.getKey();
      this.headers[index++] = entry.getValue();
    }
  }

  public String getBody() {
    return body;
  }

  public void setBody(final String body) {
    this.body = body;
  }

  public String getMethod() {
    return method;
  }

  public void setMethod(final String method) {
    this.method = method;
  }

  public Integer getStatus() {
    return status;
  }

  public void setStatus(final int status) {
    this.status = status;
  }

  @Override
  protected void writeData(final PagedByteArray buffer) {
    // no-op
  }
}
