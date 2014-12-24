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

import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.master.container.TajoContainer;

import java.util.List;

public class StageContainerAllocationEvent extends StageEvent {
  private List<TajoContainer> allocatedContainer;

  public StageContainerAllocationEvent(final ExecutionBlockId id,
                                       List<TajoContainer> allocatedContainer) {
    super(id, StageEventType.SQ_CONTAINER_ALLOCATED);
    this.allocatedContainer = allocatedContainer;
  }

  public List<TajoContainer> getAllocatedContainer() {
    return this.allocatedContainer;
  }
}
