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

import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.master.TaskRunnerGroupEvent.EventType;
import org.apache.tajo.master.container.TajoContainer;

import java.util.Collection;

public class TaskRunnerGroupEvent extends AbstractEvent<EventType> {
  public enum EventType {
    CONTAINER_REMOTE_LAUNCH,
    CONTAINER_REMOTE_CLEANUP
  }

  protected final ExecutionBlockId executionBlockId;
  protected final Collection<TajoContainer> containers;
  public TaskRunnerGroupEvent(EventType eventType,
                              ExecutionBlockId executionBlockId,
                              Collection<TajoContainer> containers) {
    super(eventType);
    this.executionBlockId = executionBlockId;
    this.containers = containers;
  }

  public Collection<TajoContainer> getContainers() {
    return containers;
  }

  public ExecutionBlockId getExecutionBlockId() {
    return executionBlockId;
  }
}
