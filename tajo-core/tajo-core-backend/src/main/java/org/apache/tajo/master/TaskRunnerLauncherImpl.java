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

package org.apache.tajo.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.tajo.SubQueryId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.TaskRunnerGroupEvent.EventType;
import org.apache.tajo.master.event.QueryEvent;
import org.apache.tajo.master.event.QueryEventType;
import org.apache.tajo.master.querymaster.QueryMaster;
import org.apache.tajo.master.querymaster.QueryMaster.QueryContext;
import org.apache.tajo.worker.TaskRunner;

import java.util.Collection;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TaskRunnerLauncherImpl extends AbstractService implements TaskRunnerLauncher {

  /** Class Logger */
  private static final Log LOG = LogFactory.getLog(TaskRunnerLauncherImpl.class);
  private QueryContext context;
  private final String queryMasterHost;
  private final int queryMasterPort;

  // For ContainerLauncherSpec
  private ContainerLaunchContext commonContainerSpec = null;

  /** for launching TaskRunners in parallel */
  private final ExecutorService executorService;

  public TaskRunnerLauncherImpl(QueryContext context) {
    super(TaskRunnerLauncherImpl.class.getName());
    this.context = context;
    queryMasterHost = context.getQueryMasterServiceAddress().getHostName();
    queryMasterPort = context.getQueryMasterServiceAddress().getPort();
    executorService = Executors.newFixedThreadPool(
        context.getConf().getIntVar(TajoConf.ConfVars.AM_TASKRUNNER_LAUNCH_PARALLEL_NUM));
  }

  public void start() {
    super.start();
  }

  public void stop() {
    executorService.shutdownNow();
    Map<ContainerId, ContainerProxy> containers = context.getContainers();
    for(ContainerProxy eachProxy: containers.values()) {
      try {
        eachProxy.kill();
      } catch (Exception e) {
      }
    }
    super.stop();
  }

  @Override
  public void handle(TaskRunnerGroupEvent event) {
    if (event.getType() == EventType.CONTAINER_REMOTE_LAUNCH) {
     launchTaskRunners(event.subQueryId, event.getContainers());
    } else if (event.getType() == EventType.CONTAINER_REMOTE_CLEANUP) {
      killTaskRunners(event.getContainers());
    }
  }

  private void launchTaskRunners(SubQueryId subQueryId, Collection<Container> containers) {
    commonContainerSpec = ContainerProxy.createCommonContainerLaunchContext(getConfig(), subQueryId.toString(), false);
    for (Container container : containers) {
      final ContainerProxy proxy =
          new TaskRunnerContainerProxy(context, getConfig(), context.getYarnRPC(), container, subQueryId);
      executorService.submit(new LaunchRunner(container.getId(), proxy));
    }
  }

  private class LaunchRunner implements Runnable {
    private final ContainerProxy proxy;
    private final ContainerId id;

    public LaunchRunner(ContainerId id, ContainerProxy proxy) {
      this.proxy = proxy;
      this.id = id;
    }
    @Override
    public void run() {
      proxy.launch(commonContainerSpec);
      LOG.info("ContainerProxy started:" + id);
    }
  }

  private void killTaskRunners(Collection<Container> containers) {
    for (Container container : containers) {
      final ContainerProxy proxy = context.getContainer(container.getId());
      executorService.submit(new KillRunner(container.getId(), proxy));
    }
  }

  private class KillRunner implements Runnable {
    private final ContainerProxy proxy;
    private final ContainerId id;
    public KillRunner(ContainerId id, ContainerProxy proxy) {
      this.id = id;
      this.proxy = proxy;
    }

    @Override
    public void run() {
      proxy.kill();
      LOG.info("ContainerProxy killed:" + id);
    }
  }

  public class TaskRunnerContainerProxy extends ContainerProxy {
    private final SubQueryId subQueryId;

    public TaskRunnerContainerProxy(QueryMaster.QueryContext context, Configuration conf, YarnRPC yarnRPC,
                                    Container container, SubQueryId subQueryId) {
      super(context, conf, yarnRPC, container);
      this.subQueryId = subQueryId;
    }

    @Override
    protected void containerStarted() {
      context.getEventHandler().handle(new QueryEvent(context.getQueryId(), QueryEventType.INIT_COMPLETED));
    }

    @Override
    protected String getId() {
      return subQueryId.toString();
    }

    @Override
    protected String getRunnerClass() {
      return TaskRunner.class.getCanonicalName();
    }

    @Override
    protected Vector<CharSequence> getTaskParams() {
      Vector<CharSequence> taskParams = new Vector<CharSequence>();
      taskParams.add(queryMasterHost); // queryMaster hostname
      taskParams.add(String.valueOf(queryMasterPort)); // queryMaster port

      return taskParams;
    }
  }
}
