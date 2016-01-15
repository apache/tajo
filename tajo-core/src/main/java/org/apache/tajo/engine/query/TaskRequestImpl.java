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

package org.apache.tajo.engine.query;

import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.ResourceProtos.TaskRequestProto;
import org.apache.tajo.ResourceProtos.FetchProto;
import org.apache.tajo.ResourceProtos.TaskRequestProtoOrBuilder;
import org.apache.tajo.plan.serder.PlanProto;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;

public class TaskRequestImpl implements TaskRequest {

	private TaskAttemptId id;
	private List<FragmentProto> fragments;
	private String outputTable;
	private boolean isUpdated;
	private boolean clusteredOutput;
	private PlanProto.LogicalNodeTree plan;     // logical node
	private Boolean interQuery;
	private List<FetchProto> fetches;
	private QueryContext queryContext;
	private DataChannel dataChannel;
	private Enforcer enforcer;
	private String queryMasterHostAndPort;
	
	private TaskRequestProto proto = TaskRequestProto.getDefaultInstance();
	private TaskRequestProto.Builder builder = null;
	private boolean viaProto = false;
	
	public TaskRequestImpl() {
		builder = TaskRequestProto.newBuilder();
		this.id = null;
		this.isUpdated = false;
	}
	
	public TaskRequestImpl(TaskAttemptId id, List<FragmentProto> fragments,
												 String outputTable, boolean clusteredOutput,
												 PlanProto.LogicalNodeTree plan, QueryContext queryContext, DataChannel channel,
												 Enforcer enforcer, String queryMasterHostAndPort) {
		this();
		this.set(id, fragments, outputTable, clusteredOutput, plan, queryContext, channel, enforcer, queryMasterHostAndPort);
	}
	
	public TaskRequestImpl(TaskRequestProto proto) {
		this.proto = proto;
		viaProto = true;
		id = null;
		isUpdated = false;
	}
	
	public void set(TaskAttemptId id, List<FragmentProto> fragments,
			String outputTable, boolean clusteredOutput,
			PlanProto.LogicalNodeTree plan, QueryContext queryContext,
									DataChannel dataChannel, Enforcer enforcer, String queryMasterHostAndPort) {
		this.id = id;
		this.fragments = fragments;
		this.outputTable = outputTable;
		this.clusteredOutput = clusteredOutput;
		this.plan = plan;
		this.isUpdated = true;
    this.queryContext = queryContext;
    this.queryContext = queryContext;
    this.dataChannel = dataChannel;
    this.enforcer = enforcer;
    this.queryMasterHostAndPort = queryMasterHostAndPort;
  }
	@Override
	public TaskRequestProto getProto() {
		mergeLocalToProto();
		proto = viaProto ? proto : builder.build();
		viaProto = true;
		return proto;
	}

	@Override
	public TaskAttemptId getId() {
		TaskRequestProtoOrBuilder p = viaProto ? proto : builder;
		if (id != null) {
			return this.id;
		}
		if (!p.hasId()) {
			return null;
		}
		this.id = new TaskAttemptId(p.getId());
		return this.id;
	}

	@Override
	public List<FragmentProto> getFragments() {
		TaskRequestProtoOrBuilder p = viaProto ? proto : builder;
		if (fragments != null) {
			return fragments;
		}
		if (fragments == null) {
			fragments = new ArrayList<>();
		}
		for (int i = 0; i < p.getFragmentsCount(); i++) {
			fragments.add(p.getFragments(i));
		}
		return this.fragments;
	}


	@Override
	public PlanProto.LogicalNodeTree getPlan() {
		TaskRequestProtoOrBuilder p = viaProto ? proto : builder;
		if (this.plan != null) {
			return this.plan;
		}
		if (!p.hasPlan()) {
			return null;
		}
		this.plan = p.getPlan();
		return this.plan;
	}

	public boolean isInterQuery() {
	  TaskRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (interQuery != null) {
      return interQuery;
    }
    if (!p.hasInterQuery()) {
      return false;
    }
    this.interQuery = p.getInterQuery();
    return this.interQuery;
	}
	
	public void setInterQuery() {
	  maybeInitBuilder();
	  this.interQuery = true;
	}

  @Override
  public void addFetch(FetchProto fetch) {
    maybeInitBuilder();
    initFetches();
    fetches.add(fetch);
  }

  public QueryContext getQueryContext(TajoConf conf) {
    TaskRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (queryContext != null) {
      return queryContext;
    }
    if (!p.hasQueryContext()) {
      return null;
    }
    this.queryContext = new QueryContext(conf, p.getQueryContext());
    return this.queryContext;
  }

  public void setQueryContext(QueryContext queryContext) {
    maybeInitBuilder();
    this.queryContext = queryContext;
  }

  public void setDataChannel(DataChannel dataChannel) {
    maybeInitBuilder();
    this.dataChannel = dataChannel;
  }

  @Override
  public DataChannel getDataChannel() {
    TaskRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (dataChannel != null) {
      return dataChannel;
    }
    if (!p.hasDataChannel()) {
      return null;
    }
    this.dataChannel = new DataChannel(p.getDataChannel());
    return this.dataChannel;
  }

  @Override
  public Enforcer getEnforcer() {
    TaskRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (enforcer != null) {
      return enforcer;
    }
    if (!p.hasEnforcer()) {
      return null;
    }
    this.enforcer = new Enforcer(p.getEnforcer());
    return this.enforcer;
  }

  @Override
  public List<FetchProto> getFetches() {
	  initFetches();
    return this.fetches;
	}
	
	private void initFetches() {
	  if (this.fetches != null) {
      return;
    }
    TaskRequestProtoOrBuilder p = viaProto ? proto : builder;
    this.fetches = new ArrayList<>();
		fetches.addAll(p.getFetchesList().stream().collect(Collectors.toList()));
	}

  private void maybeInitBuilder() {
		if (viaProto || builder == null) {
			builder = TaskRequestProto.newBuilder(proto);
		}
		viaProto = true;
	}
	
	private void mergeLocalToBuilder() {
		if (id != null) {
			builder.setId(this.id.getProto());
		}
		if (fragments != null) {
      for (FragmentProto fragment : fragments) {
        builder.addFragments(fragment);
			}
		}
		if (this.outputTable != null) {
			builder.setOutputTable(this.outputTable);
		}
		if (this.isUpdated) {
			builder.setClusteredOutput(this.clusteredOutput);
		}
		if (this.plan != null) {
			builder.setPlan(this.plan);
		}
		if (this.interQuery != null) {
		  builder.setInterQuery(this.interQuery);
		}
    if (this.fetches != null) {
      for (FetchProto fetch : fetches) {
        builder.addFetches(fetch);
      }
    }
    if (this.queryMasterHostAndPort != null) {
      builder.setQueryMasterHostAndPort(this.queryMasterHostAndPort);
    }
    if (this.queryContext != null) {
      builder.setQueryContext(queryContext.getProto());
    }
    if (this.dataChannel != null) {
      builder.setDataChannel(dataChannel.getProto());
    }
    if (this.enforcer != null) {
      builder.setEnforcer(enforcer.getProto());
    }
	}

	private void mergeLocalToProto() {
		if(viaProto) {
			maybeInitBuilder();
		}
		mergeLocalToBuilder();
		proto = builder.build();
		viaProto = true;
	}
}
