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

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.tajo.master.ContainerProxy;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractResourceAllocator extends CompositeService implements ResourceAllocator {
  private int minCapability;
  private int maxCapability;
  private int numCluster;

  private Map<ContainerId, ContainerProxy> containers = new HashMap<ContainerId, ContainerProxy>();

  public AbstractResourceAllocator() {
    super(AbstractResourceAllocator.class.getName());
  }

  public void addContainer(ContainerId cId, ContainerProxy container) {
    containers.put(cId, container);
  }

  public void removeContainer(ContainerId cId) {
    containers.remove(cId);
  }

  public boolean containsContainer(ContainerId cId) {
    return containers.containsKey(cId);
  }

  public ContainerProxy getContainer(ContainerId cId) {
    return containers.get(cId);
  }

  public Map<ContainerId, ContainerProxy> getContainers() {
    return containers;
  }

  public void setMaxContainerCapability(int capability) {
    this.maxCapability = capability;
  }

  public int getMaxContainerCapability() {
    return this.maxCapability;
  }

  public void setMinContainerCapability(int capability) {
    this.minCapability = capability;
  }

  public int getNumClusterNode() {
    return numCluster;
  }

  public void setNumClusterNodes(int num) {
    numCluster = num;
  }

}
