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

import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tajo.QueryId;
import org.apache.tajo.ResourceProtos.NodeResourceRequest;
import org.apache.tajo.ResourceProtos.AllocationResourceProto;
import org.apache.tajo.master.scheduler.event.SchedulerEvent;
import org.apache.tajo.resource.NodeResource;

import java.util.List;

/**
 * This interface is used by scheduler for allocating of resources.
 */
public interface TajoResourceScheduler extends EventHandler<SchedulerEvent> {

  /**
   * Get the whole resource capacity of the cluster.
   * @return the whole resource capacity of the cluster.
   */

  NodeResource getClusterResource();

  /**
   * Get minimum allocatable {@link NodeResource}.
   * @return minimum allocatable resource
   */
  NodeResource getMinimumResourceCapability();

  /**
   * Get minimum allocatable {@link NodeResource} of QueryMaster.
   * @return minimum allocatable resource
   */
  NodeResource getQMMinimumResourceCapability();

  /**
   * Get maximum allocatable {@link NodeResource}.
   * @return maximum allocatable resource
   */
  NodeResource getMaximumResourceCapability();

  /**
   * Get the number of nodes available in the cluster.
   * @return the number of available nodes.
   */
  int getNumClusterNodes();

  /**
   * Get reservation resource. The cluster resource is updated by TajoResourceTracker
   * Request one or more resource containers. You can set the number of containers and resource capabilities,
   * such as memory, CPU cores, and disk slots.
   * @return the number of reserved resources.
   */
  List<AllocationResourceProto>
  reserve(QueryId queryId, NodeResourceRequest ask);

}
