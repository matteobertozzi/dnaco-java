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

package tech.dnaco.tracing;

import java.lang.StackWalker.StackFrame;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import tech.dnaco.logging.Logger;
import tech.dnaco.strings.HumansUtil;

public final class Tracer {
  private static final long SLOW_TASK_DEBUG_TRESHOLD_NS = TimeUnit.MILLISECONDS.toNanos(500);

  private static ConcurrentHashMap<Thread, TaskTracer> taskTracer = new ConcurrentHashMap<>(64);

  private static final TaskTracer[] recentlyCompletedTracers = new TaskTracer[16];
  private static final AtomicLong recentlyCompletedIndex = new AtomicLong(0);

  public static Collection<TaskTracer> getActiveTasks() {
    return taskTracer.values();
  }

  public static List<TaskTracer> getRecentlyCompletedTasks() {
    final ArrayList<TaskTracer> tasks = new ArrayList<>(recentlyCompletedTracers.length);
    for (int i = 0; i < recentlyCompletedTracers.length; ++i) {
      if (recentlyCompletedTracers[i] == null) continue;
      tasks.add(recentlyCompletedTracers[i]);
    }
    tasks.sort((a, b) -> Long.compare(b.getStartTime(), a.getStartTime()));
    return tasks;
  }

  public static TaskTracer newTask(final String label) {
    final Thread thread = Thread.currentThread();
    final TaskTracer tracer = new TaskTracer(thread, lookupLogLineClassAndMethod(), label);
    taskTracer.put(thread, tracer);
    addLogEntry(tracer);
    return tracer;
  }

  protected static void closeTask(final Thread thread) {
    final TaskTracer task = taskTracer.remove(thread);
    if (task.getElapsedNs() >= SLOW_TASK_DEBUG_TRESHOLD_NS) {
      Logger.debug("slow task {} completed in {}", task.getLabel(), HumansUtil.humanTimeNanos(task.getElapsedNs()));
    }
    addCompletedTask(task);
  }

  private static void addLogEntry(final TaskTracer taskTracer) {
    /*
    final LogEntryTask logEntry = new LogEntryTask();
    logEntry.setTenantId("__SYS_TRACES__");
    logEntry.setThread(taskTracer.getThread().getName());
    logEntry.setTimestamp(taskTracer.getStartTime());
    logEntry.setTraceId(taskTracer.getTraceId());
    //Logger.add(taskTracer.getThread(), logEntry);
     */
  }

  private static String lookupLogLineClassAndMethod() {
    // Get the stack trace: this is expensive... but really useful
    // NOTE: i should be set to the first public method
    // i = 3 -> [0: lookupLogLineClassAndMethod(), 1: traceMethod(), 4:userFunc()]
    final StackFrame frame = StackWalker.getInstance().walk(s ->
      s.skip(2)
      .filter(x -> !Logger.EXCLUDE_CLASSES.contains(x.getClassName()))
      .findFirst()
    ).get();

    // com.foo.Bar.m1():11
    return frame.getClassName() + "." + frame.getMethodName() + "():" + frame.getLineNumber();
  }

  public static TraceSpan newSpan(final String label) {
    return NoOpTraceSpan.INSTANCE;
  }

  private static void addCompletedTask(final TaskTracer task) {
    final int index = (int) (recentlyCompletedIndex.incrementAndGet() & (recentlyCompletedTracers.length - 1));
    recentlyCompletedTracers[index] = task;
    addLogEntry(task);
  }
}
