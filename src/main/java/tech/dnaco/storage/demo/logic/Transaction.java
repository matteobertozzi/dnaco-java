/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package tech.dnaco.storage.demo.logic;

import java.util.UUID;

import tech.dnaco.logging.Logger;
import tech.dnaco.strings.StringFormat;

public class Transaction {
  public enum State { FAILED, PENDING, PREPARED, COMMITTED, ROLLEDBACK }

  private final String txnId;
  private final long maxSeqId;
  private final boolean local;

  private State state = State.PENDING;
  private String message;

  public Transaction(final String txnId, final long maxSeqId) {
    this(txnId, maxSeqId, false);
  }

  public Transaction(final String txnId, final long maxSeqId, final boolean local) {
    this.txnId = txnId;
    this.maxSeqId = maxSeqId;
    this.local = local;
  }

  public static Transaction newLocalTxn(final long nextCommitId) {
    return new Transaction(UUID.randomUUID().toString(), nextCommitId, true);
  }

  public boolean isLocal() {
    return local;
  }

  public Transaction setState(final State newState) {
    if (newState == State.ROLLEDBACK && this.state == State.FAILED) {
      Logger.trace("keep message {state} {newState}: {}", state, newState, message);
    } else {
      this.message = null;
    }
    this.state = newState;
    return this;
  }

  public Transaction setFailed(final String format, final Object... args) {
    this.state = State.FAILED;
    this.message = StringFormat.format(format, args);
    return this;
  }

  public State getState() {
    return state;
  }

  public String getMessage() {
    return message;
  }

  public String getTxnId() {
    return txnId;
  }

  public long getMaxSeqId() {
    return maxSeqId;
  }

  @Override
  public String toString() {
    return "Transaction [txnId=" + txnId + ", state=" + state + ", maxSeqId=" + maxSeqId + "]";
  }
}
