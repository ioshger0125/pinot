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

package com.linkedin.pinot.core.query.scheduler;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.query.ServerQueryRequest;
import com.linkedin.pinot.core.query.scheduler.resources.ResourceManager;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnull;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Priority queues of scheduler groups that determines query priority based on tokens
 *
 * This is a multi-level query scheduling queue with each sublevel maintaining a waitlist of
 * queries for the group. The priority between groups is determined based on the available
 * number of tokens. If two groups have same number of tokens then the group with lower
 * resource utilization is selected first. Oldest query from the winning SchedulerGroup
 * is selected for execution.
 *
 */
public class PriorityQueue implements SchedulerPriorityQueue {

  private static Logger LOGGER = LoggerFactory.getLogger(PriorityQueue.class);
  private static final String QUERY_DEADLINE_SECONDS_KEY = "query_deadline_seconds";
  private static final String MAX_PENDING_PER_GROUP_KEY = "max_pending_per_group";
  private static final String QUEUE_WAKEUP_MICROS = "queue_wakeup_micros";

  private static final int DEFAULT_WAKEUP_MICROS = 1000;

  private static int wakeUpTimeMicros = DEFAULT_WAKEUP_MICROS;
  private final int maxPendingPerGroup;

  private final Map<String, SchedulerGroup> schedulerGroups = new HashMap<>();
  private final Lock queueLock = new ReentrantLock();
  private final Condition queryReaderCondition = queueLock.newCondition();
  private final ResourceManager resourceManager;
  private final SchedulerGroupMapper groupSelector;
  private final int queryDeadlineMillis;
  private final SchedulerGroupFactory groupFactory;
  private final Configuration config;


  public PriorityQueue(@Nonnull Configuration config, @Nonnull ResourceManager resourceManager,
      @Nonnull  SchedulerGroupFactory groupFactory,
      @Nonnull SchedulerGroupMapper groupMapper) {
    Preconditions.checkNotNull(config);
    Preconditions.checkNotNull(resourceManager);
    Preconditions.checkNotNull(groupFactory);
    Preconditions.checkNotNull(groupMapper);

    // max available tokens per millisecond equals number of threads (total execution capacity)
    // we are over provisioning tokens here because its better to keep pipe full rather than empty
    queryDeadlineMillis = config.getInt(QUERY_DEADLINE_SECONDS_KEY, 30) * 1000;
    wakeUpTimeMicros = config.getInt(QUEUE_WAKEUP_MICROS, DEFAULT_WAKEUP_MICROS);
    maxPendingPerGroup = config.getInt(MAX_PENDING_PER_GROUP_KEY, 10);
    this.config = config;
    this.resourceManager = resourceManager;
    this.groupFactory = groupFactory;
    this.groupSelector = groupMapper;
  }

  @Override
  public void put(@Nonnull SchedulerQueryContext query) throws OutOfCapacityError {
    Preconditions.checkNotNull(query);
    queueLock.lock();
    String groupName = groupSelector.getSchedulerGroupName(query);
    try {
      SchedulerGroup groupContext = getOrCreateGroupContext(groupName);
      checkGroupHasCapacity(groupContext);
      query.setSchedulerGroupContext(groupContext);
      groupContext.addLast(query);
      queryReaderCondition.signal();
    } finally {
      queueLock.unlock();
    }
  }

  private void checkGroupHasCapacity(SchedulerGroup groupContext) throws OutOfCapacityError {
    if (groupContext.numPending() < maxPendingPerGroup &&
        groupContext.totalReservedThreads() < resourceManager.getTableThreadsHardLimit()) {
      return;
    }
    throw new OutOfCapacityError(
        String.format("SchedulerGroup %s is out of capacity. numPending: %d, maxPending: %d, reservedThreads: %d threadsHardLimit: %d",
            groupContext.name(),
            groupContext.numPending(), maxPendingPerGroup,
            groupContext.totalReservedThreads(), resourceManager.getTableThreadsHardLimit()));
  }

  public SchedulerGroup getOrCreateGroupContext(String groupName) {
    SchedulerGroup groupContext = schedulerGroups.get(groupName);
    if (groupContext == null) {
      groupContext = groupFactory.create(config, groupName);
      schedulerGroups.put(groupName, groupContext);
    }
    return groupContext;
  }

  /**
   * Blocking call to read the next query in order of priority
   * @return
   */
  @Override
  public @Nonnull SchedulerQueryContext take() {
    queueLock.lock();
    try {
      while (true) {
        SchedulerQueryContext schedulerQueryContext;
        while ( (schedulerQueryContext = takeNextInternal()) == null) {
          try {
            queryReaderCondition.await(wakeUpTimeMicros, TimeUnit.MICROSECONDS);
          } catch (InterruptedException e) {
            // continue
          }
        }
        return schedulerQueryContext;
      }
    } finally {
      queueLock.unlock();
    }
  }

  private SchedulerQueryContext takeNextInternal() {
    SchedulerGroup currentWinnerGroup = null;
    long startTime = System.nanoTime();
    StringBuilder sb = new StringBuilder("SchedulerInfo:");
    long deadlineEpochMillis = System.currentTimeMillis() - queryDeadlineMillis;
    for (Map.Entry<String, SchedulerGroup> groupInfoEntry : schedulerGroups.entrySet()) {
      SchedulerGroup group = groupInfoEntry.getValue();
      sb.append(group.toString());
      if (group.isEmpty() || !resourceManager.canSchedule(group)) {
        continue;
      }
      group.trimExpired(deadlineEpochMillis);
      if (currentWinnerGroup == null) {
        currentWinnerGroup = group;
        continue;
      }

      // Preconditions:
      // a. currentGroupResources <= hardLimit
      // b. selectedGroupResources <= hardLimit
      // We prefer group with higher tokens but with resource limits.
      // If current groupTokens are greater than currentWinningTokens then we choose current
      // group over currentWinnerGroup if
      // a. current group is using less than softLimit resources
      // b. if softLimit < currentGroupResources <= hardLimit then
      //     i. choose group if softLimit <= currentWinnerGroup <= hardLimit
      //     ii. continue with currentWinnerGroup otherwise
      int comparison = group.compareTo(currentWinnerGroup);
      if (comparison < 0) {
        continue;
      }
      if (comparison >= 0) {
        if (group.totalReservedThreads() < resourceManager.getTableThreadsSoftLimit() ||
            group.totalReservedThreads() < currentWinnerGroup.totalReservedThreads()) {
          currentWinnerGroup = group;
        }
      }
    }

    SchedulerQueryContext query = null;
    if (currentWinnerGroup != null) {
      ServerQueryRequest queryRequest = currentWinnerGroup.peekFirst().getQueryRequest();
      sb.append(String.format(" Winner: %s: [%d,%d,%d,%d]", currentWinnerGroup.name(),
          queryRequest.getTimerContext().getQueryArrivalTimeMs(),
          queryRequest.getInstanceRequest().getRequestId(),
          queryRequest.getInstanceRequest().getSearchSegments().size(),
          startTime));
      query = currentWinnerGroup.removeFirst();
    }
    LOGGER.info(sb.toString());
    return query;
  }
}

