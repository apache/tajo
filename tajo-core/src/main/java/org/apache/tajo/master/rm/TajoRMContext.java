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
import org.apache.tajo.QueryId;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;

/**
 * It's a worker resource manager context. It contains all context data about TajoWorkerResourceManager.
 */
public class TajoRMContext {

  final Dispatcher rmDispatcher;

  /** map between workerIds and running workers */
  private final ConcurrentMap<String, Worker> workers = new ConcurrentHashMap<String, Worker>();

  /** map between workerIds and inactive workers */
  private final ConcurrentMap<String, Worker> inactiveWorkers = new ConcurrentHashMap<String, Worker>();

  /** map between queryIds and query master ContainerId */
  private final ConcurrentMap<QueryId, ContainerIdProto> qmContainerMap = Maps.newConcurrentMap();

  private final Set<String> liveQueryMasterWorkerResources =
      Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

  private final Set<QueryId> stoppedQueryIds =
      Collections.newSetFromMap(new ConcurrentHashMap<QueryId, Boolean>());

  public TajoRMContext(Dispatcher dispatcher) {
    this.rmDispatcher = dispatcher;
  }

  public Dispatcher getDispatcher() {
    return rmDispatcher;
  }

  /**
   * @return The Map for active workers
   */
  public ConcurrentMap<String, Worker> getWorkers() {
    return workers;
  }

  /**
   * @return The Map for inactive workers
   */
  public ConcurrentMap<String, Worker> getInactiveWorkers() {
    return inactiveWorkers;
  }

  /**
   *
   * @return The Map for query master containers
   */
  public ConcurrentMap<QueryId, ContainerIdProto> getQueryMasterContainer() {
    return qmContainerMap;
  }

  public Set<String> getQueryMasterWorker() {
    return liveQueryMasterWorkerResources;
  }

  public Set<QueryId> getStoppedQueryIds() {
    return stoppedQueryIds;
  }
}
