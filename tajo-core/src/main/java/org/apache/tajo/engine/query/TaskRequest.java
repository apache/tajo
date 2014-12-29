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

import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.plan.serder.PlanProto;
import org.apache.tajo.worker.FetchImpl;

import java.util.List;

public interface TaskRequest extends ProtoObject<TajoWorkerProtocol.TaskRequestProto> {

	public TaskAttemptId getId();
	public List<CatalogProtos.FragmentProto> getFragments();
	public String getOutputTableId();
	public boolean isClusteredOutput();
	public PlanProto.LogicalNodeTree getPlan();
	public boolean isInterQuery();
	public void setInterQuery();
	public void addFetch(String name, FetchImpl fetch);
	public List<FetchImpl> getFetches();
  public boolean shouldDie();
  public void setShouldDie();
  public QueryContext getQueryContext(TajoConf conf);
  public DataChannel getDataChannel();
  public Enforcer getEnforcer();
}
