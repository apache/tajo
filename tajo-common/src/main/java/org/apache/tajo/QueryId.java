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

public class QueryId implements Comparable<QueryId> {
  public static final String SEPARATOR = "_";
  public static final String QUERY_ID_PREFIX = "q";

  private String id;
  private int seq;

  public QueryId(String id, int seq) {
    this.id = id;
    this.seq = seq;
  }

  public QueryId(TajoIdProtos.QueryIdProto queryId) {
    this(queryId.getId(), queryId.getSeq());
  }

  public String getId() {
    return id;
  }

  public int getSeq() {
    return seq;
  }

  @Override
  public String toString() {
    return QUERY_ID_PREFIX + SEPARATOR + toStringNoPrefix();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if(!(obj instanceof QueryId)) {
      return false;
    }
    return compareTo((QueryId)obj) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id, seq);
  }

  @Override
  public int compareTo(QueryId queryId) {
    int result = id.compareTo(queryId.id);
    if (result == 0) {
      return seq - queryId.seq;
    } else {
      return result;
    }
  }

  public TajoIdProtos.QueryIdProto getProto() {
    return TajoIdProtos.QueryIdProto.newBuilder()
        .setId(id)
        .setSeq(seq)
        .build();
  }

  public String toStringNoPrefix() {
    return id + SEPARATOR + QueryIdFactory.ID_FORMAT.format(seq);
  }
}
