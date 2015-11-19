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

package org.apache.tajo.catalog;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.json.GsonObject;

import static org.apache.tajo.catalog.proto.CatalogProtos.SortSpecProto;


public class SortSpec implements Cloneable, GsonObject, ProtoObject<SortSpecProto> {
  @Expose private Column sortKey;
  @Expose private boolean ascending = true;
  @Expose private boolean nullsFirst = false;

  public SortSpec(final Column sortKey) {
    this.sortKey = sortKey;
  }

  /**
   *
   * @param sortKey columns to sort
   * @param asc true if the sort order is ascending order
   * @param nullsFirst
   * Otherwise, it should be false.
   */
  public SortSpec(final Column sortKey, final boolean asc, final boolean nullsFirst) {
    this(sortKey);
    this.ascending = asc;
    this.nullsFirst = nullsFirst;
  }

  public SortSpec(SortSpecProto sortSpec) {
    this.sortKey = new Column(sortSpec.getColumn());
    this.ascending = sortSpec.getAscending();
    this.nullsFirst = sortSpec.getNullFirst();
  }

  public final boolean isAscending() {
    return this.ascending;
  }

  public final void setDescOrder() {
    this.ascending = false;
  }

  public final boolean isNullsFirst() {
    return this.nullsFirst;
  }

  public final void setNullsFirst() {
    this.nullsFirst = true;
  }

  public final Column getSortKey() {
    return this.sortKey;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    SortSpec key = (SortSpec) super.clone();
    key.sortKey = sortKey;
    key.ascending = ascending;
    key.nullsFirst = nullsFirst;

    return key;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(Objects.hashCode(sortKey), ascending, nullsFirst);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof SortSpec) {
      SortSpec other = (SortSpec) object;
      return sortKey.equals(other.sortKey) &&
          ascending == other.ascending &&
          nullsFirst == other.nullsFirst;
    } else {
      return false;
    }
  }

  @Override
  public String toJson() {
    return CatalogGsonHelper.toJson(this, SortSpec.class);
  }

  public String toString() {
    return sortKey + " ("+(ascending ? "asc" : "desc") + ", " + (nullsFirst ? "nulls first" : "nulls last") +")";
  }

  @Override
  public SortSpecProto getProto() {
    SortSpecProto.Builder builder = SortSpecProto.newBuilder();
    builder.setColumn(sortKey.getProto());
    builder.setAscending(ascending);
    builder.setNullFirst(nullsFirst);
    return builder.build();
  }
}
