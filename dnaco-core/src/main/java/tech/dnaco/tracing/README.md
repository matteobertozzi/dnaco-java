try (TracerTask tracer = Tracer.newTask()) {
  tracer.setData("key", "value");

  ...
}


Tracer.newTask(tenantId)

class TracerTask {
  private final Thread thread;
  private final long traceId;
  private final long startTime;

  private String[] customKeys;
  private Object[] customValues;
  private long elapsedNs;

  public TracerTask() {
    this.thread = Thread.currentThread();
    this.traceId = LogUtil.nextTraceId();
    this.startTime = System.currentTimeMillis();

    this.customKeys = null;
    this.customVals = null;
    this.elapsedNs = System.nanoTime();
  }

  public void addData(final String key, final String value) {
    if (this.customKeys != null) {
      customKeys = Arrays.copyOf(customKeys, 1 + customKeys.length);
      customVals = Arrays.copyOf(customVals, 1 + customVals.length)
    } else {
      customKeys = new String[] { key };
      customVals = new String[] { value };
    }
  }
}

interface Trace {
  traceId: string;
  threadId: number;
  startTime: number;
  elapsedTime: number;

  tenantId: string;
  operationId: string;

  // auth
  userId?: string;
  apiKey?: string;

  // info
  label: string;
  data?: any;

  // status
  failedStatus?: string;
  failedException?: string;
}