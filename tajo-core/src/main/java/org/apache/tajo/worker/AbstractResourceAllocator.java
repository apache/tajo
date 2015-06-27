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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.hadoop.service.CompositeService;
import org.apache.tajo.master.ContainerProxy;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.master.container.TajoContainerId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractResourceAllocator extends CompositeService implements ResourceAllocator {

  // resource-id to resource
  private final Map<Integer, AllocatedResource> allocated = new HashMap<Integer, AllocatedResource>();

  public synchronized void addAllocatedResource(AllocatedResource resource) {
    allocated.put(resource.getResourceId(), resource);
  }

  public synchronized AllocatedResource getAllocatedResource(int resourceId) {
    return allocated.get(resourceId);
  }

  public WorkerConnectionInfo getWorkerConnectionInfo(int resourceId) {
    return getAllocatedResource(resourceId).getConnectionInfo();
  }

  public synchronized List<AllocatedResource> removeFreeResources() {
    List<AllocatedResource> result = new ArrayList<AllocatedResource>();
    for (AllocatedResource resource : allocated.values()) {
      if (resource.acquire()) {
        result.add(resource);
      }
    }
    for (AllocatedResource free : result) {
      allocated.remove(free.getConnectionInfo().getId());
    }
    return result;
  }

  public synchronized void removeResources(List<AllocatedResource> resources) {
    for (AllocatedResource resource : resources) {
      allocated.remove(resource.getConnectionInfo().getId());
    }
  }

  public synchronized List<AllocatedResource> allocatedResources(Resources required, int count) {
    Predicate<AllocatedResource> predicate = AllocatedResource.getMinimum(required);
    List<AllocatedResource> result = new ArrayList<AllocatedResource>(count);
    for (AllocatedResource resource : Iterables.filter(allocated.values(), predicate)) {
      if (resource.acquire()) {
        result.add(resource);
      }
    }
    return result;
  }

  private Map<TajoContainerId, ContainerProxy> containers = Maps.newConcurrentMap();

  public AbstractResourceAllocator() {
    super(AbstractResourceAllocator.class.getName());
  }

  public void addContainer(TajoContainerId cId, ContainerProxy container) {
    containers.put(cId, container);
  }

  public void removeContainer(TajoContainerId cId) {
    containers.remove(cId);
  }

  public boolean containsContainer(TajoContainerId cId) {
    return containers.containsKey(cId);
  }

  public ContainerProxy getContainer(TajoContainerId cId) {
    return containers.get(cId);
  }

  public Map<TajoContainerId, ContainerProxy> getContainers() {
    return containers;
  }
}
