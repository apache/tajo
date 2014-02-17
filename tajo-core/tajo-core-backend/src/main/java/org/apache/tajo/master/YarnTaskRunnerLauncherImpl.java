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
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.TaskRunnerGroupEvent.EventType;
import org.apache.tajo.master.querymaster.QueryMasterTask;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class YarnTaskRunnerLauncherImpl extends AbstractService implements TaskRunnerLauncher {

  /** Class Logger */
  private static final Log LOG = LogFactory.getLog(YarnTaskRunnerLauncherImpl.class);
  //private final YarnRPC yarnRPC;
  private final static RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  private QueryMasterTask.QueryMasterTaskContext context;

  // For ContainerLauncherSpec
  private static AtomicBoolean initialClasspathFlag = new AtomicBoolean();
  private static String initialClasspath = null;
  private static final Object classpathLock = new Object();
  private ContainerLaunchContext commonContainerSpec = null;

  final public static FsPermission QUERYCONF_FILE_PERMISSION =
      FsPermission.createImmutable((short) 0644); // rw-r--r--

  /** for launching TaskRunners in parallel */
  private final ExecutorService executorService;

  private YarnRPC yarnRPC;

  public YarnTaskRunnerLauncherImpl(QueryMasterTask.QueryMasterTaskContext context, YarnRPC yarnRPC) {
    super(YarnTaskRunnerLauncherImpl.class.getName());
    this.context = context;
    this.yarnRPC = yarnRPC;
    executorService = Executors.newFixedThreadPool(
        context.getConf().getIntVar(TajoConf.ConfVars.YARN_RM_TASKRUNNER_LAUNCH_PARALLEL_NUM));
  }

  public void start() {
    super.start();
  }

  public void stop() {
    executorService.shutdownNow();

    Map<ContainerId, ContainerProxy> containers = context.getResourceAllocator().getContainers();
    List<ContainerProxy> list = new ArrayList<ContainerProxy>(containers.values());
    for(ContainerProxy eachProxy:  list) {
      try {
        eachProxy.stopContainer();
      } catch (Exception e) {
      }
    }
    super.stop();
  }

  @Override
  public void handle(TaskRunnerGroupEvent event) {
    if (event.getType() == EventType.CONTAINER_REMOTE_LAUNCH) {
     launchTaskRunners(event.executionBlockId, event.getContainers());
    } else if (event.getType() == EventType.CONTAINER_REMOTE_CLEANUP) {
      stopTaskRunners(event.getContainers());
    }
  }

  private void launchTaskRunners(ExecutionBlockId executionBlockId, Collection<Container> containers) {
    commonContainerSpec = YarnContainerProxy.createCommonContainerLaunchContext(getConfig(),
        executionBlockId.getQueryId().toString(), false);
    for (Container container : containers) {
      final ContainerProxy proxy = new YarnContainerProxy(context, getConfig(),
          yarnRPC, container, executionBlockId);
      executorService.submit(new LaunchRunner(container.getId(), proxy));
    }
  }

  protected class LaunchRunner implements Runnable {
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

  private void stopTaskRunners(Collection<Container> containers) {
    for (Container container : containers) {
      final ContainerProxy proxy = context.getResourceAllocator().getContainer(container.getId());
      executorService.submit(new StopContainerRunner(container.getId(), proxy));
    }
  }

  private static class StopContainerRunner implements Runnable {
    private final ContainerProxy proxy;
    private final ContainerId id;
    public StopContainerRunner(ContainerId id, ContainerProxy proxy) {
      this.id = id;
      this.proxy = proxy;
    }

    @Override
    public void run() {
      proxy.stopContainer();
      LOG.info("ContainerProxy stopped:" + id);
    }
  }


  /**
   * Lock this on initialClasspath so that there is only one fork in the AM for
   * getting the initial class-path. TODO: We already construct
   * a parent CLC and use it for all the containers, so this should go away
   * once the mr-generated-classpath stuff is gone.
   */
  private static String getInitialClasspath(Configuration conf) {
    synchronized (classpathLock) {
      if (initialClasspathFlag.get()) {
        return initialClasspath;
      }
      Map<String, String> env = new HashMap<String, String>();

      initialClasspath = env.get(Environment.CLASSPATH.name());
      initialClasspathFlag.set(true);
      return initialClasspath;
    }
  }

//  public class TaskRunnerContainerProxy extends ContainerProxy {
//    private final ExecutionBlockId executionBlockId;
//
//    public TaskRunnerContainerProxy(QueryMasterTask.QueryContext context, Configuration conf, YarnRPC yarnRPC,
//                                    Container container, ExecutionBlockId executionBlockId) {
//      super(context, conf, yarnRPC, container);
//      this.executionBlockId = executionBlockId;
//    }
//
//    @Override
//    protected void containerStarted() {
//      context.getEventHandler().handle(new QueryEvent(context.getQueryId(), QueryEventType.INIT_COMPLETED));
//    }
//
//    @Override
//    protected String getId() {
//      return executionBlockId.toString();
//    }
//
//    @Override
//    protected String getRunnerClass() {
//      return TaskRunner.class.getCanonicalName();
//    }
//
//    @Override
//    protected Vector<CharSequence> getTaskParams() {
//      Vector<CharSequence> taskParams = new Vector<CharSequence>();
//      taskParams.add(queryMasterHost); // queryMaster hostname
//      taskParams.add(String.valueOf(queryMasterPort)); // queryMaster port
//
//      return taskParams;
//    }
//  }
}
