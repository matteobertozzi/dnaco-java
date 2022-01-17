package tech.dnaco.storage.net.models;

import tech.dnaco.storage.demo.logic.Transaction;

public class TransactionStatusResponse {
  private final Transaction.State state;
  private final String message;

  public TransactionStatusResponse(final Transaction.State state, final String message) {
    this.state = state;
    this.message = message;
  }

  public Transaction.State getState() {
    return state;
  }

  public String getMessage() {
    return message;
  }
}
