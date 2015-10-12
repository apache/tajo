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

package org.apache.tajo.master.rm;

import com.google.common.collect.Maps;
import org.apache.hadoop.yarn.event.Dispatcher;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * It's a worker resource manager context. It contains all context data about TajoResourceManager.
 */
public class TajoRMContext {

  final Dispatcher rmDispatcher;

  /** map between workerIds and running nodes */
  private final ConcurrentMap<Integer, NodeStatus> nodes = Maps.newConcurrentMap();

  /** map between workerIds and inactive nodes */
  private final ConcurrentMap<Integer, NodeStatus> inactiveNodes = Maps.newConcurrentMap();

  private final Set<Integer> liveQueryMasterWorkerResources =
      Collections.newSetFromMap(new ConcurrentHashMap<>());


  public TajoRMContext(Dispatcher dispatcher) {
    this.rmDispatcher = dispatcher;
  }

  public Dispatcher getDispatcher() {
    return rmDispatcher;
  }

  /**
   * @return The Map for active nodes
   */
  public ConcurrentMap<Integer, NodeStatus> getNodes() {
    return nodes;
  }

  /**
   * @return The Map for inactive nodes
   */
  public ConcurrentMap<Integer, NodeStatus> getInactiveNodes() {
    return inactiveNodes;
  }

  public Set<Integer> getQueryMasterWorker() {
    return liveQueryMasterWorkerResources;
  }
}
