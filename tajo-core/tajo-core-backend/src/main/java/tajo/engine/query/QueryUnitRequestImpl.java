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

package tajo.engine.query;

import com.google.gson.annotations.Expose;
import tajo.QueryUnitAttemptId;
import tajo.engine.MasterWorkerProtos.Fetch;
import tajo.engine.MasterWorkerProtos.QueryUnitRequestProto;
import tajo.engine.MasterWorkerProtos.QueryUnitRequestProtoOrBuilder;
import tajo.ipc.protocolrecords.QueryUnitRequest;
import tajo.storage.Fragment;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class QueryUnitRequestImpl implements QueryUnitRequest {
	
  @Expose
	private QueryUnitAttemptId id;
  @Expose
	private List<Fragment> fragments;
  @Expose
	private String outputTable;
	private boolean isUpdated;
	@Expose
	private boolean clusteredOutput;
	@Expose
	private String serializedData;     // logical node
	@Expose
	private Boolean interQuery;
	@Expose
	private List<Fetch> fetches;
  @Expose
  private Boolean shouldDie;
	
	private QueryUnitRequestProto proto = QueryUnitRequestProto.getDefaultInstance();
	private QueryUnitRequestProto.Builder builder = null;
	private boolean viaProto = false;
	
	public QueryUnitRequestImpl() {
		builder = QueryUnitRequestProto.newBuilder();
		this.id = null;
		this.isUpdated = false;
	}
	
	public QueryUnitRequestImpl(QueryUnitAttemptId id, List<Fragment> fragments,
			String outputTable, boolean clusteredOutput, 
			String serializedData) {
		this();
		this.set(id, fragments, outputTable, clusteredOutput, serializedData);
	}
	
	public QueryUnitRequestImpl(QueryUnitRequestProto proto) {
		this.proto = proto;
		viaProto = true;
		id = null;
		isUpdated = false;
	}
	
	public void set(QueryUnitAttemptId id, List<Fragment> fragments,
			String outputTable, boolean clusteredOutput, 
			String serializedData) {
		this.id = id;
		this.fragments = fragments;
		this.outputTable = outputTable;
		this.clusteredOutput = clusteredOutput;
		this.serializedData = serializedData;
		this.isUpdated = true;
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
	public List<Fragment> getFragments() {
		QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
		if (fragments != null) {
			return fragments;
		}
		if (fragments == null) {
			fragments = new ArrayList<Fragment>();
		}
		for (int i = 0; i < p.getFragmentsCount(); i++) {
			fragments.add(new Fragment(p.getFragments(i)));
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
	
	public void addFetch(String name, URI uri) {
	  maybeInitBuilder();
	  initFetches();
	  fetches.add(
	  Fetch.newBuilder()
	    .setName(name)
	    .setUrls(uri.toString()).build());
	  
	}
	
	public List<Fetch> getFetches() {
	  initFetches();    

    return this.fetches;
	}
	
	private void initFetches() {
	  if (this.fetches != null) {
      return;
    }
    QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
    this.fetches = new ArrayList<Fetch>();
    for(Fetch fetch : p.getFetchesList()) {
      fetches.add(fetch);
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
				builder.addFragments(fragments.get(i).getProto());
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
		  builder.addAllFetches(this.fetches);
		}
    if (this.shouldDie != null) {
      builder.setShouldDie(this.shouldDie);
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

  @Override
  public void initFromProto() {
    QueryUnitRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (id == null && p.hasId()) {
      this.id = new QueryUnitAttemptId(p.getId());
    }
    if (fragments == null && p.getFragmentsCount() > 0) {
      this.fragments = new ArrayList<Fragment>();
      for (int i = 0; i < p.getFragmentsCount(); i++) {
        this.fragments.add(new Fragment(p.getFragments(i)));
      }
    }
    if (outputTable == null && p.hasOutputTable()) {
      this.outputTable = p.getOutputTable();
    }
    if (isUpdated == false && p.hasClusteredOutput()) {
      this.clusteredOutput = p.getClusteredOutput();
    }
    if (serializedData == null && p.hasSerializedData()) {
      this.serializedData = p.getSerializedData();
    }
    if (interQuery == null && p.hasInterQuery()) {
      this.interQuery = p.getInterQuery();
    }
    if (fetches == null && p.getFetchesCount() > 0) {
      this.fetches = p.getFetchesList();
    }
    if (shouldDie == null && p.getShouldDie()) {
      this.shouldDie = true;
    }
  }
  
}
