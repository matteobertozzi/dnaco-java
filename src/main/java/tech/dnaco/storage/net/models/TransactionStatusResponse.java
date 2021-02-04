package tech.dnaco.storage.net.models;

import tech.dnaco.storage.demo.logic.Transaction;

public class TransactionStatusResponse {
  private final Transaction.State state;

  public TransactionStatusResponse(final Transaction.State state) {
    this.state = state;
  }

  public Transaction.State getState() {
    return state;
  }
}
