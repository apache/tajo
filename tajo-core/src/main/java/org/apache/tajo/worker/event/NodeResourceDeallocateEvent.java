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

package org.apache.tajo.worker.event;

import org.apache.tajo.TajoProtos.NodeResourceProto;
import org.apache.tajo.resource.NodeResource;
import org.apache.tajo.worker.NodeResourceManager.Allocation;

public class NodeResourceDeallocateEvent extends NodeResourceEvent {

  private Allocation allocation;

  public NodeResourceDeallocateEvent(NodeResourceProto proto, ResourceType resourceType) {
    this(new Allocation(new NodeResource(proto)), resourceType);
  }

  public NodeResourceDeallocateEvent(Allocation resource, ResourceType resourceType) {
    super(EventType.DEALLOCATE, resourceType);
    this.allocation = resource;
  }

  public Allocation getAllocation() {
    return allocation;
  }
}
