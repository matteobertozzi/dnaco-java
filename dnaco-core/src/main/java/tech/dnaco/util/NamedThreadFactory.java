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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

public class NamedThreadFactory implements ThreadFactory {
  private static final AtomicLong POOL_NUMBER = new AtomicLong(1);

  private final AtomicLong threadNumber = new AtomicLong(1);
  private final ThreadGroup group;
  private final String namePrefix;

  public NamedThreadFactory(final String name) {
    this(currentThreadGroup(), name);
  }

  public NamedThreadFactory(final ThreadGroup group, final String name) {
    this.group = group;
    this.namePrefix = newPoolName(name) + "-";
  }

  public static String newPoolName(final String name) {
    return name + "-" + POOL_NUMBER.getAndIncrement();
  }

  public static ThreadGroup currentThreadGroup() {
    final SecurityManager s = System.getSecurityManager();
    return (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
  }

  protected String newThreadName() {
    return namePrefix + threadNumber.getAndIncrement();
  }

  @Override
  public Thread newThread(final Runnable r) {
    return new Thread(group, r, newThreadName());
  }
}
