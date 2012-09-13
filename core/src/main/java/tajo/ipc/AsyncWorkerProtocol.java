/*
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

package tajo.ipc;

import tajo.Abortable;
import tajo.Stoppable;
import tajo.engine.MasterWorkerProtos.CommandRequestProto;
import tajo.engine.MasterWorkerProtos.CommandResponseProto;
import tajo.engine.MasterWorkerProtos.QueryUnitRequestProto;
import tajo.engine.MasterWorkerProtos.ServerStatusProto;
import tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import tajo.rpc.protocolrecords.PrimitiveProtos.NullProto;

public interface AsyncWorkerProtocol extends Stoppable, Abortable {

	/**
	 * 질의 구문 및 질의 제어 정보를 LeafServer에게 전달 
	 * 
	 * @param request 질의 구문 및 질의 제어 정보
	 * @return 서브 질의에 대한 응답
	 */	
	public BoolProto requestQueryUnit(QueryUnitRequestProto request) throws Exception;
	
	public CommandResponseProto requestCommand(CommandRequestProto request);

	 /**
   * LeafServer의 상태 정보를 가져옴
   * @return ServerStatus (protocol buffer)
   * @throws
   */
  public ServerStatusProto getServerStatus(NullProto request);
}
