package tech.dnaco.dispatcher.message;

import tech.dnaco.data.DataFormat;
import tech.dnaco.strings.StringUtil;
import tech.dnaco.tracing.TraceId;
import tech.dnaco.tracing.Tracer;

public class MessageError {
  private transient int statusCode;
  private TraceId traceId;
  private String status;
  private String message;
  private Object data;

  public MessageError(final String status, final String message) {
    this(status, message, null);
  }

  public MessageError(final String status, final String message, final Object data) {
    this(500, status, message, data);
  }

  public MessageError(final int statusCode, final String status, final String message) {
    this(statusCode, status, message, null);
  }

  public MessageError(final int statusCode, final String status, final String message, final Object data) {
    this(statusCode, Tracer.getCurrentTraceId(), status, message, data);
  }

  public MessageError(final int statusCode, final TraceId traceId, final String status, final String message, final Object data) {
    this.statusCode = statusCode;
    this.traceId = traceId;
    this.status = status;
    this.message = message;
    this.data = data;
  }

  public MessageError(final int statusCode) {
    this(statusCode, null, null, null);
  }

  public MessageError() {
    // used by jackson deserialize
  }

  public boolean hasBody() {
    return StringUtil.isNotEmpty(status);
  }

  public static MessageError fromBytes(final DataFormat format, final int statusCode, final byte[] data) {
    final MessageError error = format.fromBytes(data, MessageError.class);
    error.statusCode = statusCode;
    return error;
  }

  public int statusCode() { return statusCode; }
  public TraceId traceId() { return traceId; }
  public String status() { return status; }
  public String message() { return message; }
  public Object data() { return data; }

  private static MessageError INTERNAL_SERVER_ERROR = new MessageError(500, "INTERNAL_SERVER_ERROR", "internal server error");
  public static MessageError internalServerError() {
    return INTERNAL_SERVER_ERROR;
  }

  private static MessageError NOT_MODIFIED = new MessageError(304);
  public static MessageError notModified() {
    return NOT_MODIFIED;
  }

  public static MessageError newBadRequestError(final String message) {
    return new MessageError(400, "BAD_REQUEST", message);
  }

  public static MessageError newUnauthorized(final String message) {
    return new MessageError(401, "UNAUTHORIZED", message);
  }

  public static MessageError newForbidden(final String message) {
    return new MessageError(403, "FORBIDDEN", message);
  }

  private static MessageError NOT_FOUND_ERROR = new MessageError(404, "NOT_FOUND", "not found");
  public static MessageError notFound() {
    return NOT_FOUND_ERROR;
  }

  public static MessageError newNotFound(final String message) {
    return new MessageError(404, "NOT_FOUND", message);
  }

  public static MessageError newExpectationFailed(final String status, final String message) {
    return new MessageError(417, status, message);
  }

  public static MessageError newInternalServerError(final String message) {
    return new MessageError(500, "INTERNAL_SERVER_ERROR", message);
  }

  private static MessageError NOT_IMPLEMENTED_ERROR = new MessageError(501, "NOT_IMPLEMENTED", "not implemented");
  public static MessageError notImplemented() {
    return NOT_IMPLEMENTED_ERROR;
  }

  public static MessageError newNotImplemented(final String message) {
    return new MessageError(501, "NOT_IMPLEMENTED", message);
  }

  @Override
  public String toString() {
    return "MessageError [status=" + status + ", statusCode=" + statusCode
        + ", traceId=" + traceId + ", message=" + message + ", data=" + data + "]";
  }
}
