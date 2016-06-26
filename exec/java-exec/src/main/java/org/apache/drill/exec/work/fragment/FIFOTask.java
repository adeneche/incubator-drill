/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.work.fragment;

import com.google.common.base.Stopwatch;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.FragmentStats;
import org.apache.drill.exec.proto.ExecProtos;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class FIFOTask implements Runnable, Comparable<FIFOTask> {
  private final static AtomicInteger sequencer = new AtomicInteger();
  private final Runnable delegate;
  private final ExecProtos.FragmentHandle handle;
  private final FragmentStats stats;
  private final int rank;

  private final long timeAddedToQueue;

  private FIFOTask(final Runnable delegate, final ExecProtos.FragmentHandle handle, final FragmentStats stats) {
    this.delegate = delegate;
    this.handle = handle;
    this.rank = sequencer.getAndIncrement();
    this.timeAddedToQueue = System.currentTimeMillis();
    this.stats = stats;
  }

  @Override
  public void run() {
    stats.addTimeInQueue(System.currentTimeMillis() - timeAddedToQueue);
    Stopwatch watch = Stopwatch.createStarted();
    delegate.run();
    stats.setConsecutiveRuntime(watch.elapsed(TimeUnit.MILLISECONDS));
  }

  @Override
  public int compareTo(final FIFOTask other) {
    if (handle.getQueryId().equals(other.handle.getQueryId())) {
      final int result = handle.getMajorFragmentId() - other.handle.getMajorFragmentId();
      // break ties in fifo order
      if (result != 0) {
        return result;
      }
    }
    return rank - other.rank;
  }

  public static FIFOTask of(final Runnable delegate, final FragmentContext context) {
    return new FIFOTask(delegate, context.getHandle(), context.getStats());
  }

  public static FIFOTask of(final FragmentExecutor delegate) {
    final FragmentContext context = delegate.getContext();
    return new FIFOTask(delegate, context.getHandle(), context.getStats());
  }
}
