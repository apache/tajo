/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo;

import com.google.common.base.Objects;
import tajo.TajoIdProtos.QueryUnitIdProto;
import tajo.TajoIdProtos.QueryUnitIdProtoOrBuilder;
import tajo.common.ProtoObject;
import tajo.util.TajoIdUtils;

import java.text.NumberFormat;

/**
 * @author Hyunsik Choi
 */
public class QueryUnitId implements Comparable<QueryUnitId>, 
  ProtoObject<QueryUnitIdProto> {
  private static final String PREFIX = "t";

  static final ThreadLocal<NumberFormat> queryUnitIdFormat =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          fmt.setGroupingUsed(false);
          fmt.setMinimumIntegerDigits(6);
          return fmt;
        }
      };
  
  private SubQueryId subQueryId = null;
  private int id = -1;
  private String finalId = null;
  
  private QueryUnitIdProto proto = QueryUnitIdProto.getDefaultInstance();
  private QueryUnitIdProto.Builder builder = null;
  private boolean viaProto = false;
  
  public QueryUnitId() {
    builder = QueryUnitIdProto.newBuilder();
  }
  
  public QueryUnitId(final SubQueryId subQueryId,
      final int id) {
    this.subQueryId = subQueryId;
    this.id = id;
  }
  
  public QueryUnitId(QueryUnitIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public QueryUnitId(final String finalId) {
    this.finalId = finalId;
    int i = finalId.lastIndexOf(QueryId.SEPARATOR);
    this.subQueryId = TajoIdUtils.newSubQueryId(finalId.substring(0, i));
    this.id = Integer.valueOf(finalId.substring(i+1));
  }
  
  public int getId() {
    QueryUnitIdProtoOrBuilder p = viaProto ? proto : builder;
    if (this.id != -1) {
      return this.id;
    }
    if (!p.hasId()) {
      return -1;
    }
    this.id = p.getId();
    return id;
  }
  
  public SubQueryId getSubQueryId() {
    QueryUnitIdProtoOrBuilder p = viaProto ? proto : builder;
    if (this.subQueryId != null) {
      return this.subQueryId;
    }
    if (!p.hasSubQueryId()) {
      return null;
    }
    this.subQueryId = TajoIdUtils.newSubQueryId(p.getSubQueryId());
    return this.subQueryId;
  }
  
  public QueryId getQueryId() {
    return this.getSubQueryId().getQueryId();
  }
  
  @Override
  public final String toString() {
    if (finalId == null) {
      StringBuilder sb = new StringBuilder(PREFIX);
      QueryId appId = getSubQueryId().getQueryId();
      sb.append(QueryId.SEPARATOR).append(
          appId.getApplicationId().getClusterTimestamp())
      .append(QueryId.SEPARATOR).append(
          QueryId.appIdFormat.get().format(appId.getApplicationId().getId()))
      .append(QueryId.SEPARATOR).append(
          QueryId.attemptIdFormat.get().format(appId.getAttemptId()))
      .append(QueryId.SEPARATOR).append(
          SubQueryId.subQueryIdFormat.get().format(getSubQueryId().getId()))
      .append(QueryId.SEPARATOR).append(queryUnitIdFormat.get().format(getId()));
      finalId = sb.toString();
    }
    return this.finalId;
  }
  
  @Override
  public final boolean equals(final Object o) {
    if (o instanceof QueryUnitId) {
      QueryUnitId other = (QueryUnitId) o;
      return getSubQueryId().equals(other.getSubQueryId()) &&
          getId() == other.getId();
    }    
    return false;
  }
  
  @Override
  public int hashCode() {
    return Objects.hashCode(getSubQueryId(), getId());
  }

  @Override
  public final int compareTo(final QueryUnitId o) {
    return this.toString().compareTo(o.toString());
  }
  
  private void mergeProtoToLocal() {
    QueryUnitIdProtoOrBuilder p = viaProto ? proto : builder;
    if (subQueryId == null) {
      subQueryId = TajoIdUtils.newSubQueryId(p.getSubQueryId());
    }
    if (id == -1) {
      id = p.getId();
    }
  }

  @Override
  public void initFromProto() {
    mergeProtoToLocal();
  }
  
  private void mergeLocalToBuilder() {
    if (builder == null) {
      builder = QueryUnitIdProto.newBuilder(proto);
    }
    if (this.subQueryId != null) {
      builder.setSubQueryId(subQueryId.getProto());
    }
    if (this.id != -1) {
      builder.setId(id);
    }
  }

  @Override
  public QueryUnitIdProto getProto() {
    if (!viaProto) {
      mergeLocalToBuilder();
      proto = builder.build();
      viaProto = true;
    }
    return proto;
  }
}
