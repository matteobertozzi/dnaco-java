package tech.dnaco.tracing;

public class StandardAttributes {
  public static final String HOST_ID = "host.id";
  public static final String HOST_NAME = "host.name";
  public static final String HOST_TYPE = "host.type";
  public static final String HOST_IMAGE_NAME = "host.image.name";
  public static final String HOST_IMAGE_ID = "host.image.id";
  public static final String HOST_IMAGE_VERSION = "host.image.version";

  public static final String CLOUD_PROVIDER = "cloud.provider";
  public static final String CLOUD_REGION = "cloud.region";
  public static final String CLOUD_ZONE = "cloud.zone";

  public static final String OS_ARCH = "os.arch";
  public static final String OS_NAME = "os.name";
  public static final String OS_VERSION = "os.version";

  public static final String PROCESS_PID = "process.pid";
  public static final String PROCESS_OWNER = "process.owner";
  public static final String PROCESS_RUNTIME_NAME = "process.runtime.name";
  public static final String PROCESS_RUNTIME_VERSION = "process.runtime.version";

  private StandardAttributes() {
    // no-op
  }
}
