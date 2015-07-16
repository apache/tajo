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


import com.google.protobuf.RpcCallback;
import org.apache.tajo.ResourceProtos.AllocationResourceProto;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;

public class QMResourceAllocateEvent extends NodeResourceEvent {

  private AllocationResourceProto request;
  private RpcCallback<PrimitiveProtos.BoolProto> callback;

  public QMResourceAllocateEvent(AllocationResourceProto request,
                                 RpcCallback<PrimitiveProtos.BoolProto> callback) {
    super(EventType.ALLOCATE, ResourceType.QUERY_MASTER);
    this.callback = callback;
    this.request = request;
  }

  public AllocationResourceProto getRequest() {
    return request;
  }

  public RpcCallback<PrimitiveProtos.BoolProto> getCallback() {
    return callback;
  }
}
