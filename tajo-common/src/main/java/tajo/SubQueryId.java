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

import tajo.TajoIdProtos.SubQueryIdProto;
import tajo.TajoIdProtos.SubQueryIdProtoOrBuilder;

import java.text.NumberFormat;

public class SubQueryId implements Comparable<SubQueryId> {
  public static final String PREFIX = "sq";

  static final ThreadLocal<NumberFormat> subQueryIdFormat =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          fmt.setGroupingUsed(false);
          fmt.setMinimumIntegerDigits(2);
          return fmt;
        }
      };

  private SubQueryIdProto proto = SubQueryIdProto.getDefaultInstance();
  private SubQueryIdProto.Builder builder = null;
  private boolean viaProto = false;

  private QueryId queryId = null;

  public SubQueryId() {
    builder = SubQueryIdProto.newBuilder(proto);
  }

  public SubQueryId(SubQueryIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * @return the subquery number.
   */
  public synchronized int getId() {
    SubQueryIdProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getId());
  }

  public synchronized void setId(int id) {
    maybeInitBuilder();
    builder.setId((id));
  }

  /**
   * @return the associated <code>QueryId</code>
   */
  public synchronized QueryId getQueryId() {
    SubQueryIdProtoOrBuilder p = viaProto ? proto : builder;
    if (this.queryId != null) {
      return this.queryId;
    }
    if (!p.hasQueryId()) {
      return null;
    }
    queryId = new QueryId(p.getQueryId());
    return queryId;
  }

  public synchronized void setQueryId(QueryId queryId) {
    maybeInitBuilder();
    if (queryId == null)
      builder.clearQueryId();
    this.queryId = queryId;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + getId();
    result = prime * result + getQueryId().hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    SubQueryId other = (SubQueryId) obj;
    if (getId() != other.getId())
      return false;
    if (!getQueryId().equals(other.getQueryId()))
      return false;
    return true;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(PREFIX);
    QueryId queryId = getQueryId();
    builder.append(QueryId.SEPARATOR).append(queryId.getApplicationId().getClusterTimestamp());
    builder.append(QueryId.SEPARATOR).append(
        QueryId.appIdFormat.get().format(queryId.getApplicationId().getId()));
    builder.append(QueryId.SEPARATOR).append(QueryId.attemptIdFormat.get().format(queryId.getAttemptId()))
        .append(QueryId.SEPARATOR)
    .append(subQueryIdFormat.get().format(getId()));
    return builder.toString();
  }

  @Override
  public int compareTo(SubQueryId other) {
    int queryIdComp = this.getQueryId().compareTo(other.getQueryId());
    if (queryIdComp == 0) {
      return this.getId() - other.getId();
    } else {
      return queryIdComp;
    }
  }

  public synchronized SubQueryIdProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void mergeLocalToBuilder() {
    if (this.queryId != null
        && !this.queryId.getProto().equals(builder.getQueryId())) {
      builder.setQueryId(queryId.getProto());
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
}