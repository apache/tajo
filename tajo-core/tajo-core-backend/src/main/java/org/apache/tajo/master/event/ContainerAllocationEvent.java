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

package org.apache.tajo.master.event;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.tajo.ExecutionBlockId;

public class ContainerAllocationEvent extends AbstractEvent<ContainerAllocatorEventType>  {

  private final ExecutionBlockId executionBlockId;
  private final Priority priority;
  private final Resource resource;
  private final boolean isLeafQuery;
  private final int requiredNum;
  private final float progress;

  public ContainerAllocationEvent(ContainerAllocatorEventType eventType,
                                  ExecutionBlockId executionBlockId,
                                  Priority priority,
                                  Resource resource,
                                  int requiredNum,
                                  boolean isLeafQuery, float progress) {
    super(eventType);
    this.executionBlockId = executionBlockId;
    this.priority = priority;
    this.resource = resource;
    this.requiredNum = requiredNum;
    this.isLeafQuery = isLeafQuery;
    this.progress = progress;
  }

  public ExecutionBlockId getExecutionBlockId() {
    return executionBlockId;
  }

  public Priority getPriority() {
    return priority;
  }

  public int getRequiredNum() {
    return requiredNum;
  }

  public boolean isLeafQuery() {
    return isLeafQuery;
  }

  public Resource getCapability() {
    return resource;
  }

  public float getProgress() {
    return progress;
  }

  public Resource getResource() {
    return resource;
  }
}
