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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;
import com.linkedin.pinot.common.metrics.ServerMeter;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.metrics.ServerQueryPhase;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.query.ServerQueryRequest;
import com.linkedin.pinot.core.query.scheduler.resources.QueryExecutorService;
import com.linkedin.pinot.core.query.scheduler.resources.ResourceManager;
import java.util.concurrent.Semaphore;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Schedules queries from a SchedulerGroup with highest number of tokens on priority
 */
public abstract class PriorityScheduler extends QueryScheduler {
  private static Logger LOGGER = LoggerFactory.getLogger(PriorityScheduler.class);

  private final SchedulerPriorityQueue queryQueue;
  private final Semaphore runningQueriesSemaphore;

  public PriorityScheduler(@Nonnull ResourceManager resourceManager, @Nonnull QueryExecutor queryExecutor,
      @Nonnull SchedulerPriorityQueue queue, @Nonnull ServerMetrics metrics) {
    super(queryExecutor, resourceManager, metrics);
    Preconditions.checkNotNull(queue);
    this.queryQueue = queue;
    runningQueriesSemaphore = new Semaphore(resourceManager.getNumQueryRunnerThreads());
  }

  @Override
  public ListenableFuture<byte[]> submit(@Nullable final ServerQueryRequest queryRequest) {
    Preconditions.checkNotNull(queryRequest);
    if (! isRunning) {
      return internalErrorResponse(queryRequest);
    }
    queryRequest.getTimerContext().startNewPhaseTimer(ServerQueryPhase.SCHEDULER_WAIT);
    final SchedulerQueryContext schedQueryContext = new SchedulerQueryContext(queryRequest);
    try {
      queryQueue.put(schedQueryContext);
    } catch (OutOfCapacityError e) {
      LOGGER.error("Out of capacity for table {}, message: {}", queryRequest.getTableName(), e.getMessage());
      return internalErrorResponse(queryRequest);
    }
    serverMetrics.addMeteredTableValue(queryRequest.getTableName(), ServerMeter.QUERIES, 1);
    return schedQueryContext.getResultFuture();
  }


  @Override
  public void start() {
    super.start();
    Thread scheduler = new Thread(new Runnable() {
      @Override
      public void run() {
        while (isRunning) {
          try {
            runningQueriesSemaphore.acquire();
          } catch (InterruptedException e) {
            LOGGER.error("Failed to acquire semaphore. Exiting.", e);
            break;
          }
          try {
            final SchedulerQueryContext request = queryQueue.take();
            ServerQueryRequest queryRequest = request.getQueryRequest();
            final QueryExecutorService executor = resourceManager.getExecutorService(queryRequest,
                request.getSchedulerGroup());
            final ListenableFutureTask<byte[]> queryFutureTask = createQueryFutureTask(queryRequest, executor);
            queryFutureTask.addListener(new Runnable() {
              @Override
              public void run() {
                executor.releaseWorkers();
                request.getSchedulerGroup().endQuery();
                runningQueriesSemaphore.release();
              }
            }, MoreExecutors.directExecutor());
            request.setResultFuture(queryFutureTask);
            request.getSchedulerGroup().startQuery();
            queryRequest.getTimerContext().getPhaseTimer(ServerQueryPhase.SCHEDULER_WAIT).stopAndRecord();
            resourceManager.getQueryRunners().submit(queryFutureTask);
            MoreExecutors.directExecutor().execute(queryFutureTask);
          } catch (Throwable t){
            LOGGER.error("Error in scheduler thread. This is indicative of a bug. Please report this. Server will continue with errors", t);
          }
        }
        if (isRunning) {
          throw new RuntimeException("FATAL: Scheduler thread is quitting.....something went horribly wrong.....!!!");
        }
      }
    });
    scheduler.setName("ptb");
    scheduler.setPriority(Thread.MAX_PRIORITY);
    scheduler.start();
  }
}
