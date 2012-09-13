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

import tajo.QueryUnitId;
import tajo.engine.MasterWorkerProtos.QueryStatus;
import tajo.engine.MasterWorkerProtos.QueryUnitResponseProto;
import tajo.engine.MasterWorkerProtos.QueryUnitResponseProtoOrBuilder;
import tajo.ipc.protocolrecords.QueryUnitResponse;

public class QueryUnitResponseImpl implements QueryUnitResponse {
	
	private QueryUnitId id;
	private QueryStatus status;
	
	private QueryUnitResponseProto proto = QueryUnitResponseProto.getDefaultInstance();
	private QueryUnitResponseProto.Builder builder = null;
	private boolean viaProto = false;
	
	public QueryUnitResponseImpl() {
		builder = QueryUnitResponseProto.newBuilder();
		this.id = null;
	}
	
	public QueryUnitResponseImpl(QueryUnitId id, QueryStatus status) {
		this.id = id;
		this.status = status;
	}
	
	public QueryUnitResponseImpl(QueryUnitResponseProto proto) {
		this.proto = proto;
		viaProto = true;
	}

	/* (non-Javadoc)
	 * @see ProtoObject#getProto()
	 */
	@Override
	public QueryUnitResponseProto getProto() {
		mergeLocalToProto();
		proto = viaProto ? proto : builder.build();
		viaProto = true;
		return proto;
	}

	/* (non-Javadoc)
	 * @see QueryUnitResponse#getStatus()
	 */
	@Override
	public QueryStatus getStatus() {
		QueryUnitResponseProtoOrBuilder p = viaProto ? proto : builder;
		if (this.status != null) {
			return this.status;
		}
		if (!proto.hasStatus()) {
			return null;
		}
		this.status = p.getStatus();
		return this.status;
	}

	@Override
	public QueryUnitId getId() {
		QueryUnitResponseProtoOrBuilder p = viaProto ? proto : builder;
		if (id != null) {
			return this.id;
		}
		if (!proto.hasId()) {
			return null;
		}
		this.id = new QueryUnitId(p.getId());
		return this.id;
	}

	private void maybeInitBuilder() {
		if (viaProto || builder == null) {
			builder = QueryUnitResponseProto.newBuilder(proto);
		}
		viaProto = true;
	}
	
	private void mergeLocalToBuilder() {
		if (id != null) {
			builder.setId(this.id.toString());
		}
		if (this.status != null) {
			builder.setStatus(this.status);
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
    QueryUnitResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.id == null && p.hasId()) {
      this.id = new QueryUnitId(p.getId());
    }
    if (this.status == null && p.hasStatus()) {
      this.status = p.getStatus();
    }
  }
}
