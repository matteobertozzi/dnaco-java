package tech.dnaco.dispatcher;

public class DispatchOnShardException extends DispatchLaterException {
  private final Invokable executor;
  private final int shardHash;

  public DispatchOnShardException(final int shardHash, final Invokable executor) {
    this.executor = executor;
    this.shardHash = shardHash;
	}

  public int shardHash() {
    return shardHash;
  }

  public Invokable executor() {
    return executor;
  }
}
