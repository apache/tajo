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
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.querymaster.QueryMasterTask;
import org.apache.tajo.master.container.TajoContainer;
import org.apache.tajo.master.container.TajoContainerId;

public abstract class ContainerProxy {
  protected static final Log LOG = LogFactory.getLog(ContainerProxy.class);

  final public static FsPermission QUERYCONF_FILE_PERMISSION =
          FsPermission.createImmutable((short) 0644); // rw-r--r--


  protected static enum ContainerState {
    PREP, FAILED, RUNNING, DONE, KILLED_BEFORE_LAUNCH
  }

  protected final ExecutionBlockId executionBlockId;
  protected Configuration conf;
  protected QueryMasterTask.QueryMasterTaskContext context;

  protected ContainerState state;
  // store enough information to be able to cleanup the container
  protected TajoContainer container;
  protected TajoContainerId containerId;
  protected String hostName;
  protected int port = -1;

  public abstract void launch(ContainerLaunchContext containerLaunchContext);
  public abstract void stopContainer();

  public ContainerProxy(QueryMasterTask.QueryMasterTaskContext context, Configuration conf,
                        ExecutionBlockId executionBlockId, TajoContainer container) {
    this.context = context;
    this.conf = conf;
    this.state = ContainerState.PREP;
    this.container = container;
    this.executionBlockId = executionBlockId;
    this.containerId = container.getId();
  }

  public synchronized boolean isCompletelyDone() {
    return state == ContainerState.DONE || state == ContainerState.FAILED;
  }

  public String getTaskHostName() {
    return this.hostName;
  }

  public int getTaskPort() {
    return this.port;
  }

  public TajoContainerId getContainerId() {
    return containerId;
  }

  public ExecutionBlockId getBlockId() {
    return executionBlockId;
  }
}
