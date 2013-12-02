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

package org.apache.tajo.worker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.client.YarnClient;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.TaskRunnerGroupEvent;
import org.apache.tajo.master.TaskRunnerLauncher;
import org.apache.tajo.master.YarnTaskRunnerLauncherImpl;
import org.apache.tajo.master.event.ContainerAllocatorEventType;
import org.apache.tajo.master.querymaster.QueryMasterTask;
import org.apache.tajo.master.rm.YarnRMContainerAllocator;

public class YarnResourceAllocator extends AbstractResourceAllocator {
  private YarnRMContainerAllocator rmAllocator;

  private TaskRunnerLauncher taskRunnerLauncher;

  private YarnRPC yarnRPC;

  private YarnClient yarnClient;

  private static final Log LOG = LogFactory.getLog(YarnResourceAllocator.class.getName());

  private QueryMasterTask.QueryMasterTaskContext queryTaskContext;

  private TajoConf systemConf;

  public YarnResourceAllocator(QueryMasterTask.QueryMasterTaskContext queryTaskContext) {
    this.queryTaskContext = queryTaskContext;
  }

  @Override
  public ContainerId makeContainerId(YarnProtos.ContainerIdProto containerId) {
    return new ContainerIdPBImpl(containerId);
  }

  @Override
  public void allocateTaskWorker() {
  }

  @Override
  public int calculateNumRequestContainers(TajoWorker.WorkerContext workerContext,
                                           int numTasks,
                                           int memoryMBPerTask) {
    int numClusterNodes = workerContext.getNumClusterNodes();

    TajoConf conf =  (TajoConf)workerContext.getQueryMaster().getConfig();
    int workerNum = conf.getIntVar(TajoConf.ConfVars.YARN_RM_WORKER_NUMBER_PER_NODE);
    return numClusterNodes == 0 ? numTasks: Math.min(numTasks, numClusterNodes * workerNum);
  }

  @Override
  public void init(Configuration conf) {
    systemConf = (TajoConf)conf;

    yarnRPC = YarnRPC.create(systemConf);

    connectYarnClient();

    taskRunnerLauncher = new YarnTaskRunnerLauncherImpl(queryTaskContext, yarnRPC);
    addService((org.apache.hadoop.yarn.service.Service) taskRunnerLauncher);
    queryTaskContext.getDispatcher().register(TaskRunnerGroupEvent.EventType.class, taskRunnerLauncher);

    rmAllocator = new YarnRMContainerAllocator(queryTaskContext);
    addService(rmAllocator);
    queryTaskContext.getDispatcher().register(ContainerAllocatorEventType.class, rmAllocator);
    super.init(conf);
  }

  @Override
  public void stop() {
    try {
      this.yarnClient.stop();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
    super.stop();
  }

  @Override
  public void start() {
    super.start();
  }

  private void connectYarnClient() {
    this.yarnClient = new YarnClientImpl();
    this.yarnClient.init(systemConf);
    this.yarnClient.start();
  }

}
