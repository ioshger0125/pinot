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

package com.linkedin.pinot.core.query.scheduler.tokenbucket;

/**
 * Schedules queries from a SchedulerGroup with highest number of tokens on priority
 */
/*
public class TokenBucketScheduler extends QueryScheduler {
  private static Logger LOGGER = LoggerFactory.getLogger(TokenBucketScheduler.class);

  private final TokenPriorityQueue2 queryQueue;
  private final Semaphore runningQueriesSemaphore;

  public TokenBucketScheduler(@Nonnull Configuration config, QueryExecutor queryExecutor,
      ServerMetrics serverMetrics) {
    super(queryExecutor, new PolicyBasedResourceManager(config), serverMetrics);
    queryQueue = new TokenPriorityQueue2(config, resourceManager);
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
            final QueryExecutorService executor = resourceManager
                .getExecutorService(queryRequest, request.getSchedulerGroup());
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
            resourceManager.getQueryRunners().submit(queryFutureTask);
            MoreExecutors.directExecutor().execute(queryFutureTask);
          } catch (Throwable t){
            LOGGER.error("Error in scheduler thread. This is indicative of a bug. Please report this. Server will continue with errors", t);
          }
        }
        throw new RuntimeException("FATAL: Scheduler thread is quitting.....something went horribly wrong.....!!!");
      }
    });
    scheduler.setName("ptb");
    scheduler.setPriority(Thread.MAX_PRIORITY);
    scheduler.start();
  }

  @Override
  public String name() {
    return "PriorityTokenBucketScheduler";
  }
}
*/
