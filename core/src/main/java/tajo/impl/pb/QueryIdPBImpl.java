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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos;
import tajo.QueryId;
import tajo.common.ProtoObject;
import tajo.engine.TCommonProtos.QueryIdProto;
import tajo.engine.TCommonProtos.QueryIdProtoOrBuilder;

import java.text.NumberFormat;

/**
 * The protocol implementation of QueryId
 */
public class QueryIdPBImpl extends QueryId implements ProtoObject<QueryIdProto> {
  public static final String PREFIX = "query";
  public static final String SEPARATOR = "_";

  static final ThreadLocal<NumberFormat> queryIdFormat =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          fmt.setGroupingUsed(false);
          fmt.setMinimumIntegerDigits(4);
          return fmt;
        }
      };

  private QueryIdProto proto = QueryIdProto.getDefaultInstance();
  private QueryIdProto.Builder builder = null;
  private boolean viaProto = false;

  private ApplicationId appId;

  public QueryIdPBImpl() {
    builder = QueryIdProto.newBuilder();
  }

  public QueryIdPBImpl(QueryIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public synchronized ApplicationId getAppId() {
    QueryIdProtoOrBuilder p = viaProto ? proto : builder;
    if (appId != null) {
      return appId;
    } // Else via proto
    if (!p.hasAppId()) {
      return null;
    }
    appId = convertFromProtoFormat(p.getAppId());
    return appId;
  }

  @Override
  public synchronized void setAppId(ApplicationId appId) {
    maybeInitBuilder();
    if (appId == null) {
      builder.clearAppId();
    }
    this.appId = appId;
  }

  @Override
  public synchronized int getId() {
    QueryIdProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getId());
  }

  @Override
  public synchronized void setId(int id) {
    maybeInitBuilder();
    builder.setId((id));
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = QueryIdProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private synchronized void mergeLocalToBuilder() {
    if (this.appId != null
        && !((ApplicationIdPBImpl) this.appId).getProto().equals(
        builder.getAppId())) {
      builder.setAppId(convertToProtoFormat(this.appId));
    }
  }

  @Override
  public QueryIdProto getProto() {
    if (!viaProto) {
      mergeLocalToBuilder();
      proto = builder.build();
      viaProto = true;
    }
    return proto;
  }

  private void mergeProtoToLocal() {
    QueryIdProtoOrBuilder p = viaProto ? proto : builder;
    if (appId == null) {
      appId = convertFromProtoFormat(p.getAppId());
    }
  }

  @Override
  public void initFromProto() {
    mergeProtoToLocal();
  }

  private ApplicationIdPBImpl convertFromProtoFormat(
      YarnProtos.ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private YarnProtos.ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl) t).getProto();
  }
}