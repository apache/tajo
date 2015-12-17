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
package org.apache.tajo.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.util.TUtil;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * AsyncTaskService executes some blocking tasks in TajoMaster
 *
 * @See https://issues.apache.org/jira/browse/TAJO-2022
 */
public class AsyncTaskService extends AbstractService {
  private final TajoMaster.MasterContext context;
  private long TERMINATION_WAIT_TIME_SEC;
  private ExecutorService executor;

  /**
   * Construct the service.
   *
   * @param context
   */
  public AsyncTaskService(TajoMaster.MasterContext context) {
    super("MasterAsyncTaskExecutor");
    this.context = context;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    TajoConf systemConf = TUtil.checkTypeAndGet(conf, TajoConf.class);
    TERMINATION_WAIT_TIME_SEC = systemConf.getLongVar(ConfVars.MASTER_ASYNC_TASK_TERMINATION_WAIT_TIME);
    executor = Executors.newFixedThreadPool(systemConf.getIntVar(ConfVars.MASTER_ASYNC_TASK_THREAD_NUM));

    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    executor.shutdown();
    boolean terminated = false;
    try {
      terminated = executor.awaitTermination(TERMINATION_WAIT_TIME_SEC, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
    }
    if (!terminated) {
      executor.shutdownNow();
    }

    super.serviceStop();
  }

  public TajoMaster.MasterContext getMasterContext() {
    return this.context;
  }

  /**
   * Returns a new CompletableFuture that is asynchronously completed
   * by a task running in AsyncTaskService.
   *
   * @param task Task
   * @param <T> Return Type
   * @return CompletableFuture
   */
  public <T> CompletableFuture<T> supply(Supplier<T> task) {
    return CompletableFuture.supplyAsync(task, executor);
  }

  /**
   * Returns a new CompletableFuture that is asynchronously completed
   * by a task running in the given executor after it runs the given
   * action.
   *
   * @param task Task
   * @return CompletableFuture
   */
  public CompletableFuture<Void> run(Runnable task) {
    return CompletableFuture.runAsync(task, executor);
  }
}
