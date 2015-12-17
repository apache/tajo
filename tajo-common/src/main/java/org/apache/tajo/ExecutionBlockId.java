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

public class ExecutionBlockId implements Comparable<ExecutionBlockId> {
  public static final String EB_ID_PREFIX = "eb";
  private QueryId queryId;
  private int id;

  public ExecutionBlockId(QueryId queryId, int id) {
    this.queryId = queryId;
    this.id = id;
  }

  public ExecutionBlockId(TajoIdProtos.ExecutionBlockIdProto proto) {
    this(new QueryId(proto.getQueryId()),  proto.getId());
  }

  @Override
  public String toString() {
      return EB_ID_PREFIX + QueryId.SEPARATOR + toStringNoPrefix();
  }

  @Override
  public int compareTo(ExecutionBlockId executionBlockId) {
    int result = queryId.compareTo(executionBlockId.queryId);
    if (result == 0) {
      return id - executionBlockId.id;
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
    if(!(obj instanceof ExecutionBlockId)) {
      return false;
    }
    return compareTo((ExecutionBlockId)obj) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(queryId, id);
  }

  public TajoIdProtos.ExecutionBlockIdProto getProto() {
    return TajoIdProtos.ExecutionBlockIdProto.newBuilder()
        .setQueryId(queryId.getProto())
        .setId(id)
        .build();
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public int getId() {
    return id;
  }

  public String toStringNoPrefix() {
    return queryId.toStringNoPrefix() + QueryId.SEPARATOR + QueryIdFactory.EB_ID_FORMAT.format(id);
  }
}
