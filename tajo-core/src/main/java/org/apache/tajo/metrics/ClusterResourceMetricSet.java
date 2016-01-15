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

package org.apache.tajo.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.rm.NodeStatus;
import org.apache.tajo.master.rm.NodeState;
import org.apache.tajo.metrics.Master.Cluster;

import java.util.HashMap;
import java.util.Map;

public class ClusterResourceMetricSet implements MetricSet {
  TajoMaster.MasterContext masterContext;
  public ClusterResourceMetricSet(TajoMaster.MasterContext masterContext) {
    this.masterContext = masterContext;
  }

  @Override
  public Map<String, Metric> getMetrics() {
    Map<String, Metric> metricsMap = new HashMap<>();

    metricsMap.put(Cluster.TOTAL_NODES.name(), (Gauge<Integer>) () -> masterContext.getResourceManager().getNodes().size());

    metricsMap.put(Cluster.ACTIVE_NODES.name(), (Gauge<Integer>) () -> getNumWorkers(NodeState.RUNNING));

    metricsMap.put(Cluster.LOST_NODES.name(), (Gauge<Integer>) () -> getNumWorkers(NodeState.LOST));

    metricsMap.put(Cluster.TOTAL_MEMORY.name(), (Gauge<Integer>) () -> masterContext.getResourceManager().getScheduler().getMaximumResourceCapability().getMemory());

    metricsMap.put(Cluster.FREE_MEMORY.name(), (Gauge<Integer>) () -> masterContext.getResourceManager().getScheduler().getClusterResource().getMemory());

    metricsMap.put(Cluster.TOTAL_VCPU.name(), (Gauge<Integer>) () -> masterContext.getResourceManager().getScheduler().getMaximumResourceCapability().getVirtualCores());

    metricsMap.put(Cluster.FREE_VCPU.name(), (Gauge<Integer>) () -> masterContext.getResourceManager().getScheduler().getClusterResource().getVirtualCores());

    return metricsMap;
  }

  protected int getNumWorkers(NodeState status) {
    int numWorkers = 0;
    for(NodeStatus eachNodeStatus : masterContext.getResourceManager().getNodes().values()) {
      if(eachNodeStatus.getState() == status) {
        numWorkers++;
      }
    }

    return numWorkers;
  }
}
