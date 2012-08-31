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

package tajo.impl.pb;

import tajo.QueryId;
import tajo.SubQueryId;
import tajo.engine.TCommonProtos;
import tajo.engine.TCommonProtos.QueryIdProto;
import tajo.engine.TCommonProtos.SubQueryIdProto;
import tajo.engine.TCommonProtos.SubQueryIdProtoOrBuilder;

public class SubQueryIdPBImpl extends SubQueryId {
  SubQueryIdProto proto = SubQueryIdProto.getDefaultInstance();
  SubQueryIdProto.Builder builder = null;
  boolean viaProto = false;

  private QueryId jobId = null;

  public SubQueryIdPBImpl() {
    builder = SubQueryIdProto.newBuilder(proto);
  }

  public SubQueryIdPBImpl(SubQueryIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public synchronized SubQueryIdProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void mergeLocalToBuilder() {
    if (this.jobId != null
        && !((QueryIdPBImpl) this.jobId).getProto().equals(builder.getQueryId())) {
      builder.setQueryId(convertToProtoFormat(this.jobId));
    }
  }

  private synchronized void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SubQueryIdProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public synchronized int getId() {
    TCommonProtos.SubQueryIdProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getId());
  }

  @Override
  public synchronized void setId(int id) {
    maybeInitBuilder();
    builder.setId((id));
  }

  @Override
  public synchronized QueryId getQueryId() {
    SubQueryIdProtoOrBuilder p = viaProto ? proto : builder;
    if (this.jobId != null) {
      return this.jobId;
    }
    if (!p.hasQueryId()) {
      return null;
    }
    jobId = convertFromProtoFormat(p.getQueryId());
    return jobId;
  }

  @Override
  public synchronized void setQueryId(QueryId queryId) {
    maybeInitBuilder();
    if (queryId == null)
      builder.clearQueryId();
    this.jobId = queryId;
  }

  private QueryIdPBImpl convertFromProtoFormat(QueryIdProto p) {
    return new QueryIdPBImpl(p);
  }

  private TCommonProtos.QueryIdProto convertToProtoFormat(QueryId t) {
    return ((QueryIdPBImpl)t).getProto();
  }
}