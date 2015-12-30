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

/**
 * 
 */
package org.apache.tajo.engine.query;

import org.apache.tajo.ResourceProtos.FetchProto;
import org.apache.tajo.ResourceProtos.TaskRequestProto;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.plan.serder.PlanProto;

import java.util.List;

public interface TaskRequest extends ProtoObject<TaskRequestProto> {

	TaskAttemptId getId();
	List<CatalogProtos.FragmentProto> getFragments();
	PlanProto.LogicalNodeTree getPlan();
	void setInterQuery();
	void addFetch(FetchProto fetch);
	List<FetchProto> getFetches();
  QueryContext getQueryContext(TajoConf conf);
  DataChannel getDataChannel();
  Enforcer getEnforcer();
}
