package tech.dnaco.storage.net.models;

public class Scanner {
  public static final Scanner EMPTY = new Scanner(null);

  private final ScanResult result;
  private final String scannerId;

  public Scanner(final ScanResult result) {
    this(null, result);
  }

  public Scanner(final String scannerId, final ScanResult result) {
    this.result = result;
    this.scannerId = scannerId;
  }

  public ScanResult getResult() {
    return result;
  }

  public String getScannerId() {
    return scannerId;
  }
}
