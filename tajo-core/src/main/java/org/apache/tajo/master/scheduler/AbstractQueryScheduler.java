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

import org.apache.hadoop.service.AbstractService;
import org.apache.tajo.QueryId;
import org.apache.tajo.resource.NodeResource;
import org.apache.tajo.resource.NodeResources;
import org.apache.tajo.resource.ResourceCalculator;

/**
 * please refer to {@TajoResourceScheduler} for detailed information.
 */
public abstract class AbstractQueryScheduler extends AbstractService implements TajoResourceScheduler {

  protected final NodeResource clusterResource;
  protected final NodeResource minResource;
  protected final NodeResource maxResource;
  protected final NodeResource qmMinResource;

  public AbstractQueryScheduler(String name) {
    super(name);
    this.minResource = NodeResources.createResource(0);
    this.qmMinResource = NodeResources.createResource(0);
    this.maxResource = NodeResources.createResource(0);
    this.clusterResource = NodeResources.createResource(0);
  }

  @Override
  public NodeResource getClusterResource() {
    return clusterResource;
  }

  @Override
  public NodeResource getMinimumResourceCapability() {
    return minResource;
  }

  @Override
  public NodeResource getMaximumResourceCapability() {
    return maxResource;
  }

  @Override
  public NodeResource getQMMinimumResourceCapability() {
    return qmMinResource;
  }

  public abstract int getRunningQuery();

  public abstract ResourceCalculator getResourceCalculator();

  public abstract void submitQuery(QuerySchedulingInfo schedulingInfo);

  public abstract void stopQuery(QueryId queryId);

}
