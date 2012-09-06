/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

/**
 * 
 */
package tajo.engine.ipc;

import tajo.engine.MasterWorkerProtos.*;
import tajo.rpc.Callback;
import tajo.rpc.protocolrecords.PrimitiveProtos.NullProto;

public interface AsyncWorkerClientInterface {

	/**
	 * 질의 구문 및 질의 제어 정보를 LeafServer에게 전달 
	 * 
	 * @param request 질의 구문 및 질의 제어 정보
	 * @return 서브 질의에 대한 응답
	 */	
	public void requestQueryUnit(Callback<SubQueryResponseProto> callback, QueryUnitRequestProto request) throws Exception;
	
	public void requestCommand(Callback<CommandResponseProto> callback, CommandRequestProto request);
	
  /**
   * LeafServer의 상태 정보를 가져옴
   * 
   * @param callback
   * @return ServerStatus (protocol buffer)
   * @throws
   */
  public void getServerStatus(Callback<ServerStatusProto> callback, NullProto request);
}
