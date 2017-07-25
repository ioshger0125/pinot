/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.scheduler.fcfs;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.core.query.scheduler.SchedulerGroup;
import com.linkedin.pinot.core.query.scheduler.SchedulerGroupAccountant;
import com.linkedin.pinot.core.query.scheduler.SchedulerQueryContext;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;


public class FCFSGroup implements SchedulerGroup {
  private final String name;
  // Queue of pending queries
  private final ConcurrentLinkedQueue<SchedulerQueryContext> pendingQueries = new ConcurrentLinkedQueue<>();
  private AtomicInteger numRunning = new AtomicInteger(0);
  private AtomicInteger threadsInUse = new AtomicInteger(0);
  private AtomicInteger reservedThreads = new AtomicInteger(0);

  public FCFSGroup(@Nonnull String group) {
    Preconditions.checkNotNull(group);
    this.name = group;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public void addLast(SchedulerQueryContext query) {
    pendingQueries.add(query);
  }

  @Override
  public SchedulerQueryContext peekFirst() {
    return pendingQueries.peek();
  }

  @Override
  public SchedulerQueryContext removeFirst() {
    return pendingQueries.poll();
  }

  @Override
  public void trimExpired(long deadlineMillis) {
    Iterator<SchedulerQueryContext> iter = pendingQueries.iterator();
    while (iter.hasNext()) {
      SchedulerQueryContext next = iter.next();
      if (next.getArrivalTimeMs() < deadlineMillis) {
        iter.remove();
      }
    }
  }

  @Override
  public boolean isEmpty() {
    return pendingQueries.isEmpty();
  }

  @Override
  public int numPending() {
    return pendingQueries.size();
  }

  @Override
  public int numRunning() {
    return numRunning.get();
  }

  @Override
  public void incrementThreads() {
    threadsInUse.incrementAndGet();
  }

  @Override
  public void decrementThreads() {
    threadsInUse.decrementAndGet();
  }

  @Override
  public int getThreadsInUse() {
    return threadsInUse.get();
  }

  @Override
  public void addReservedThreads(int threads) {
    reservedThreads.addAndGet(threads);
  }

  @Override
  public void releasedReservedThreads(int threads) {
    reservedThreads.addAndGet(-1 * threads);
  }

  @Override
  public int totalReservedThreads() {
    return reservedThreads.get();
  }

  @Override
  public void startQuery() {
    incrementThreads();
    numRunning.incrementAndGet();
  }

  @Override
  public void endQuery() {
    decrementThreads();
    numRunning.decrementAndGet();
  }

  @Override
  public int compareTo(SchedulerGroupAccountant rhs) {
    if (rhs == null) {
      return 1;
    }

    if (this == rhs) {
      return 0;
    }
    long lhsArrivalMs = peekFirst().getArrivalTimeMs();
    long rhsArrivalMs = ((FCFSGroup)rhs).peekFirst().getArrivalTimeMs();
    if (lhsArrivalMs < rhsArrivalMs) {
      return -1;
    } else if (lhsArrivalMs > rhsArrivalMs) {
      return 1;
    }
    return 0;
  }
}
