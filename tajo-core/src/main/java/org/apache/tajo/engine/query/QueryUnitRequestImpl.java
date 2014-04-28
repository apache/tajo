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

import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.ipc.TajoWorkerProtocol.QueryUnitRequestProto;
import org.apache.tajo.ipc.TajoWorkerProtocol.QueryUnitRequestProtoOrBuilder;
import org.apache.tajo.worker.FetchImpl;

import java.util.ArrayList;
import java.util.List;

import static org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;

public class QueryUnitRequestImpl implements QueryUnitRequest {
	
  private QueryUnitAttemptId id;
  private List<FragmentProto> fragments;
  private String outputTable;
	private boolean isUpdated;
	private boolean clusteredOutput;
	private String serializedData;     // logical node
	private Boolean interQuery;
	private List<FetchImpl> fetches;
  private Boolean shouldDie;
  private QueryContext queryContext;
  private DataChannel dataChannel;
  private Enforcer enforcer;
	
	private QueryUnitRequestProto proto = QueryUnitRequestProto.getDefaultInstance();
	private QueryUnitRequestProto.Builder builder = null;
	private boolean viaProto = false;
	
	public QueryUnitRequestImpl() {
		builder = QueryUnitRequestProto.newBuilder();
		this.id = null;
		this.isUpdated = false;
	}
	
	public QueryUnitRequestImpl(QueryUnitAttemptId id, List<FragmentProto> fragments,
			String outputTable, boolean clusteredOutput,
			String serializedData, QueryContext queryContext, DataChannel channel, Enforcer enforcer) {
		this();
		this.set(id, fragments, outputTable, clusteredOutput, serializedData, queryContext, channel, enforcer);
	}
	
	public QueryUnitRequestImpl(QueryUnitRequestProto proto) {
		this.proto = proto;
		viaProto = true;
		id = null;
		isUpdated = false;
	}
	
	public void set(QueryUnitAttemptId id, List<FragmentProto> fragments,
			String outputTable, boolean clusteredOutput,
			String serializedData, QueryContext queryContext, DataChannel dataChannel, Enforcer enforcer) {
		this.id = id;
		this.fragments = fragments;
		this.outputTable = outputTable;
		this.clusteredOutput = clusteredOutput;
		this.serializedData = serializedData;
		this.isUpdated = true;
    this.queryContext = queryContext;
    this.queryContext = queryContext;
    this.dataChannel = dataChannel;
    this.enforcer = enforcer;
	}

	@Override
	public QueryUnitRequestProto getProto() {
		mergeLocalToProto();
		proto = viaProto ? proto : builder.build();
		viaProto = true;
		return proto;
	}

	@Override
	public QueryUnitAttemptId getId() {
		QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
		if (id != null) {
			return this.id;
		}
		if (!p.hasId()) {
			return null;
		}
		this.id = new QueryUnitAttemptId(p.getId());
		return this.id;
	}

	@Override
	public List<FragmentProto> getFragments() {
		QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
		if (fragments != null) {
			return fragments;
		}
		if (fragments == null) {
			fragments = new ArrayList<FragmentProto>();
		}
		for (int i = 0; i < p.getFragmentsCount(); i++) {
			fragments.add(p.getFragments(i));
		}
		return this.fragments;
	}

	@Override
	public String getOutputTableId() {
		QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
		if (outputTable != null) {
			return this.outputTable;
		}
		if (!p.hasOutputTable()) {
			return null;
		}
		this.outputTable = p.getOutputTable();
		return this.outputTable;
	}

	@Override
	public boolean isClusteredOutput() {
		QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
		if (isUpdated) {
			return this.clusteredOutput;
		}
		if (!p.hasClusteredOutput()) {
			return false;
		}
		this.clusteredOutput = p.getClusteredOutput();
		this.isUpdated = true;
		return this.clusteredOutput;
	}

	@Override
	public String getSerializedData() {
		QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
		if (this.serializedData != null) {
			return this.serializedData;
		}
		if (!p.hasSerializedData()) {
			return null;
		}
		this.serializedData = p.getSerializedData();
		return this.serializedData;
	}
	
	public boolean isInterQuery() {
	  QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
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

  public void addFetch(String name, FetchImpl fetch) {
    maybeInitBuilder();
    initFetches();
    fetch.setName(name);
    fetches.add(fetch);
  }

  public QueryContext getQueryContext() {
    QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (queryContext != null) {
      return queryContext;
    }
    if (!p.hasQueryContext()) {
      return null;
    }
    this.queryContext = new QueryContext(p.getQueryContext());
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
    QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
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
    QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (enforcer != null) {
      return enforcer;
    }
    if (!p.hasEnforcer()) {
      return null;
    }
    this.enforcer = new Enforcer(p.getEnforcer());
    return this.enforcer;
  }

  public List<FetchImpl> getFetches() {
	  initFetches();    

    return this.fetches;
	}
	
	private void initFetches() {
	  if (this.fetches != null) {
      return;
    }
    QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
    this.fetches = new ArrayList<FetchImpl>();
    for(TajoWorkerProtocol.FetchProto fetch : p.getFetchesList()) {
      fetches.add(new FetchImpl(fetch));
    }
	}

  @Override
  public boolean shouldDie() {
    QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (shouldDie != null) {
      return shouldDie;
    }
    if (!p.hasShouldDie()) {
      return false;
    }
    this.shouldDie = p.getShouldDie();
    return this.shouldDie;
  }

  @Override
  public void setShouldDie() {
    maybeInitBuilder();
    shouldDie = true;
  }

  private void maybeInitBuilder() {
		if (viaProto || builder == null) {
			builder = QueryUnitRequestProto.newBuilder(proto);
		}
		viaProto = true;
	}
	
	private void mergeLocalToBuilder() {
		if (id != null) {
			builder.setId(this.id.getProto());
		}
		if (fragments != null) {
			for (int i = 0; i < fragments.size(); i++) {
				builder.addFragments(fragments.get(i));
			}
		}
		if (this.outputTable != null) {
			builder.setOutputTable(this.outputTable);
		}
		if (this.isUpdated) {
			builder.setClusteredOutput(this.clusteredOutput);
		}
		if (this.serializedData != null) {
			builder.setSerializedData(this.serializedData);
		}
		if (this.interQuery != null) {
		  builder.setInterQuery(this.interQuery);
		}
    if (this.fetches != null) {
      for (int i = 0; i < fetches.size(); i++) {
        builder.addFetches(fetches.get(i).getProto());
      }
    }
    if (this.shouldDie != null) {
      builder.setShouldDie(this.shouldDie);
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
