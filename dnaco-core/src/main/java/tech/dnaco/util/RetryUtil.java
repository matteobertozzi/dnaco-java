/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.dnaco.util;

public final class RetryUtil {
  private RetryUtil() {
    // no-op
  }

  public static RetryLogic newFixedRetry(final int intervalMillis) {
    return newFixedRetry(intervalMillis, 1000);
  }

  public static RetryLogic newFixedRetry(final int intervalMillis, final int jitterMillis) {
    return new FixedRetry(intervalMillis, jitterMillis);
  }

  public static RetryLogic newExponentialRetry(final int minWaitMillis, final int maxWaitMillis) {
    return newExponentialRetry(minWaitMillis, maxWaitMillis, 1000);
  }

  public static RetryLogic newExponentialRetry(final int minWaitMillis, final int maxWaitMillis, final int jitterMillis) {
    return new ExponentialRetry(minWaitMillis, maxWaitMillis, jitterMillis);
  }

  public interface RetryLogic {
    int minWaitIntervalMillis();
    int maxWaitIntervalMillis();
    int nextWaitIntervalMillis();
    void reset();
  }

  private static abstract class AbstractCountingRetry implements RetryLogic {
    private int attemptCount = 1;

    public void reset() {
      this.attemptCount = 1;
    }

    protected int newAttempt() {
      return attemptCount++;
    }
  }

  private static abstract class BasicMinMaxRetry extends AbstractCountingRetry {
    private final int minWaitInterval;
    private final int maxWaitInterval;

    protected BasicMinMaxRetry(final int minWaitIntervalMillis, final int maxWaitIntervalMillis) {
      this.maxWaitInterval = maxWaitIntervalMillis;
      this.minWaitInterval = minWaitIntervalMillis;
    }

    @Override
    public int minWaitIntervalMillis() {
      return minWaitInterval;
    }

    @Override
    public int maxWaitIntervalMillis() {
      return maxWaitInterval;
    }
  }

  private static final class FixedRetry extends AbstractCountingRetry {
    private final int intervalMillis;
    private final int jitterMillis;

    public FixedRetry(final int intervalMillis, final int jitterMillis) {
      this.intervalMillis = intervalMillis;
      this.jitterMillis = jitterMillis;
    }

    @Override
    public int minWaitIntervalMillis() {
      return intervalMillis;
    }

    @Override
    public int maxWaitIntervalMillis() {
      return intervalMillis;
    }

    @Override
    public int nextWaitIntervalMillis() {
      final int jitter = (int) Math.round(jitterMillis * Math.random());
      return (Math.random() > 0.5) ? intervalMillis - jitter : intervalMillis + jitter;
    }
  }

  private static final class ExponentialRetry extends BasicMinMaxRetry {
    private final int jitterMillis;

    public ExponentialRetry(final int minWaitIntervalMillis, final int maxWaitIntervalMillis, final int jitterMillis) {
      super(minWaitIntervalMillis, maxWaitIntervalMillis);
      this.jitterMillis = jitterMillis;
    }

    @Override
    public int nextWaitIntervalMillis() {
      final long interval = Math.round(minWaitIntervalMillis() * Math.pow(2, newAttempt()));
      return (int) (Math.min(interval, maxWaitIntervalMillis()) + Math.round(jitterMillis * Math.random()));
    }
  }
}
