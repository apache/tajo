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

import com.google.gson.annotations.SerializedName;
import org.apache.tajo.util.TUtil;

public class Sort extends UnaryOperator {
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
  public boolean equalsTo(Expr expr) {
    Sort another = (Sort) expr;
    return TUtil.checkEquals(sortSpecs, another.sortSpecs);
  }

  @Override
  public String toJson() {
    return JsonHelper.toJson(this);
  }

  public static class SortSpec {
    private ColumnReferenceExpr key;
    private boolean asc = true;
    @SerializedName("null_first")
    private boolean nullFirst = false;

    public SortSpec(final ColumnReferenceExpr key) {
      this.key = key;
    }

    /**
     *
     * @param sortKey a column to sort
     * @param asc true if the sort order is ascending order
     * @param nullFirst
     * Otherwise, it should be false.
     */
    public SortSpec(final ColumnReferenceExpr sortKey, final boolean asc,
                    final boolean nullFirst) {
      this(sortKey);
      this.asc = asc;
      this.nullFirst = nullFirst;
    }

    public final boolean isAscending() {
      return this.asc;
    }

    public final void setDescending() {
      this.asc = false;
    }

    public final boolean isNullFirst() {
      return this.nullFirst;
    }

    public final void setNullFirst() {
      this.nullFirst = true;
    }

    public final ColumnReferenceExpr getKey() {
      return this.key;
    }

    public boolean equals(Object obj) {
      if (obj instanceof SortSpec) {
        SortSpec other = (SortSpec) obj;
        return TUtil.checkEquals(key, other.key) &&
            TUtil.checkEquals(asc, other.asc) &&
            TUtil.checkEquals(nullFirst, other.nullFirst);
      }
      return false;
    }
  }
}
