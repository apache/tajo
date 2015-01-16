/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.master.scheduler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.QueryId;
import org.apache.tajo.master.QueryInProgress;
import org.apache.tajo.master.QueryManager;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleFifoScheduler implements Scheduler {
  private static final Log LOG = LogFactory.getLog(SimpleFifoScheduler.class.getName());
  private LinkedList<QuerySchedulingInfo> pool = new LinkedList<QuerySchedulingInfo>();
  private final Thread queryProcessor;
  private AtomicBoolean stopped = new AtomicBoolean();
  private QueryManager manager;
  private Comparator<QuerySchedulingInfo> COMPARATOR = new SchedulingAlgorithms.FifoComparator();

  public SimpleFifoScheduler(QueryManager manager) {
    this.manager = manager;
    this.queryProcessor = new Thread(new QueryProcessor());
    this.queryProcessor.setName("Query Processor");
  }

  @Override
  public Mode getMode() {
    return Mode.FIFO;
  }

  @Override
  public String getName() {
    return manager.getName();
  }

  @Override
  public boolean addQuery(QueryInProgress queryInProgress) {
    int qSize = pool.size();
    if (qSize != 0 && qSize % 100 == 0) {
      LOG.info("Size of Fifo queue is " + qSize);
    }

    QuerySchedulingInfo querySchedulingInfo = new QuerySchedulingInfo(queryInProgress.getQueryId(), 1,
        queryInProgress.getQueryInfo().getStartTime());
    boolean result = pool.add(querySchedulingInfo);
    if (getRunningQueries().size() == 0) wakeupProcessor();
    return result;
  }

  @Override
  public boolean removeQuery(QueryId queryId) {
    return pool.remove(getQueryByQueryId(queryId));
  }

  public QuerySchedulingInfo getQueryByQueryId(QueryId queryId) {
    for (QuerySchedulingInfo querySchedulingInfo : pool) {
      if (querySchedulingInfo.getQueryId().equals(queryId)) {
        return querySchedulingInfo;
      }
    }
    return null;
  }

  @Override
  public List<QueryInProgress> getRunningQueries() {
    return new ArrayList<QueryInProgress>(manager.getRunningQueries());
  }

  public void start() {
    queryProcessor.start();
  }

  public void stop() {
    if (stopped.getAndSet(true)) {
      return;
    }
    pool.clear();
    synchronized (queryProcessor) {
      queryProcessor.interrupt();
    }
  }

  private QuerySchedulingInfo pollScheduledQuery() {
    if (pool.size() > 1) {
      Collections.sort(pool, COMPARATOR);
    }
    return pool.poll();
  }

  private void wakeupProcessor() {
    synchronized (queryProcessor) {
      queryProcessor.notifyAll();
    }
  }

  private final class QueryProcessor implements Runnable {
    @Override
    public void run() {

      QuerySchedulingInfo query;

      while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
        query = null;
        if (getRunningQueries().size() == 0) {
          query = pollScheduledQuery();
        }

        if (query != null) {
          try {
            manager.startQueryJob(query.getQueryId());
          } catch (Throwable t) {
            LOG.fatal("Exception during query startup:", t);
            manager.stopQuery(query.getQueryId());
          }
        }

        synchronized (queryProcessor) {
          try {
            queryProcessor.wait(500);
          } catch (InterruptedException e) {
            if (stopped.get()) {
              break;
            }
            LOG.warn("Exception during shutdown: ", e);
          }
        }
      }
    }
  }
}
