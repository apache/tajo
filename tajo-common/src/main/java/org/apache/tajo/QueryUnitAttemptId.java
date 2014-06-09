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

package org.apache.tajo;

import com.google.common.base.Objects;

public class QueryUnitAttemptId implements Comparable<QueryUnitAttemptId> {
  public static final String QUA_ID_PREFIX = "ta";

  private QueryUnitId queryUnitId;
  private int id;

  public QueryUnitId getQueryUnitId() {
    return queryUnitId;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public QueryUnitAttemptId(QueryUnitId queryUnitId, int id) {
    this.queryUnitId = queryUnitId;
    this.id = id;
  }

  public QueryUnitAttemptId(TajoIdProtos.QueryUnitAttemptIdProto proto) {
    this(new QueryUnitId(proto.getQueryUnitId()), proto.getId());
  }

  public TajoIdProtos.QueryUnitAttemptIdProto getProto() {
    return TajoIdProtos.QueryUnitAttemptIdProto.newBuilder()
        .setQueryUnitId(queryUnitId.getProto())
        .setId(id)
        .build();
  }

  @Override
  public int compareTo(QueryUnitAttemptId queryUnitAttemptId) {
    int result = queryUnitId.compareTo(queryUnitAttemptId.queryUnitId);
    if (result == 0) {
      return id - queryUnitAttemptId.id;
    } else {
      return result;
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if(!(obj instanceof QueryUnitAttemptId)) {
      return false;
    }
    return compareTo((QueryUnitAttemptId)obj) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(queryUnitId, id);
  }

  @Override
  public String toString() {
    return QUA_ID_PREFIX + QueryId.SEPARATOR + toStringNoPrefix();
  }

  public String toStringNoPrefix() {
    return queryUnitId.toStringNoPrefix() + QueryId.SEPARATOR + QueryIdFactory.ATTEMPT_ID_FORMAT.format(id);
  }
}
