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

package org.apache.tajo.algebra;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.tajo.util.TUtil;

public class Sort extends UnaryOperator {
  @Expose @SerializedName("SortSpecs")
  private SortSpec [] sortSpecs;

  public Sort(final SortSpec [] sortSpecs) {
    super(OpType.Sort);
    this.sortSpecs = sortSpecs;
  }

  public void setSortSpecs(SortSpec[] sortSpecs) {
    this.sortSpecs = sortSpecs;
  }

  public SortSpec [] getSortSpecs() {
    return this.sortSpecs;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(sortSpecs);
  }

  @Override
  public boolean equalsTo(Expr expr) {
    Sort another = (Sort) expr;
    return TUtil.checkEquals(sortSpecs, another.sortSpecs);
  }

  @Override
  public String toJson() {
    return JsonHelper.toJson(this);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    Sort sort = (Sort) super.clone();
    sort.sortSpecs = new SortSpec[sortSpecs.length];
    for (int i = 0; i < sortSpecs.length; i++) {
      sort.sortSpecs[i] = (SortSpec) sortSpecs[i].clone();
    }
    return sort;
  }

  public static class SortSpec implements Cloneable {
    @Expose @SerializedName("SortKey")
    private Expr key;
    @Expose @SerializedName("IsAsc")
    private boolean asc = true;
    @Expose @SerializedName("IsNullsFirst")
    // The default value for nulls order is decided based on the sort order.
    // That is, nulls appear at the bottom with asc and the top with desc.
    private Boolean nullsFirst = null;

    public SortSpec(final Expr key) {
      this.key = key;
    }

    /**
     *
     * @param sortKey a column to sort
     * @param asc true if the sort order is ascending order
     * @param nullsFirst
     * Otherwise, it should be false.
     */
    public SortSpec(final ColumnReferenceExpr sortKey, final boolean asc,
                    final boolean nullsFirst) {
      this(sortKey);
      this.asc = asc;
      this.nullsFirst = nullsFirst;
    }

    public final boolean isAscending() {
      return this.asc;
    }

    public final void setDescending() {
      this.asc = false;
    }

    public final boolean isNullsFirst() {
      if (nullsFirst == null) {
        return !asc;
      } else {
        return nullsFirst;
      }
    }

    public final void setNullsFirst() {
      this.nullsFirst = true;
    }

    public final void setNullsLast() {
      this.nullsFirst = false;
    }

    public void setKey(Expr expr) {
      this.key = expr;
    }

    public final Expr getKey() {
      return this.key;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(asc, key, nullsFirst);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof SortSpec) {
        SortSpec other = (SortSpec) obj;
        return TUtil.checkEquals(key, other.key) &&
            TUtil.checkEquals(asc, other.asc) &&
            TUtil.checkEquals(nullsFirst, other.nullsFirst);
      }
      return false;
    }

    @Override
    public String toString() {
      return key + " " + (asc ? "asc" : "desc") + " " + (isNullsFirst() ? "nulls first" : "nulls last");
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
      SortSpec sortSpec = (SortSpec) super.clone();
      sortSpec.key = (Expr) key.clone();
      sortSpec.asc = asc;
      sortSpec.nullsFirst = nullsFirst;
      return sortSpec;
    }
  }
}
