/*
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

package tajo.algebra;

import tajo.util.TUtil;

public class Sort extends UnaryOperator {
  private SortSpec [] sort_specs;

  public Sort(final SortSpec [] sortSpecs) {
    super(ExprType.Sort);
    this.sort_specs = sortSpecs;
  }

  public void setSortSpecs(SortSpec[] sort_specs) {
    this.sort_specs = sort_specs;
  }

  public SortSpec [] getSortSpecs() {
    return this.sort_specs;
  }

  @Override
  public boolean equalsTo(Expr expr) {
    Sort another = (Sort) expr;
    return TUtil.checkEquals(sort_specs, another.sort_specs);
  }

  @Override
  public String toJson() {
    return JsonHelper.toJson(this);
  }

  public static class SortSpec {
    private ColumnReferenceExpr key;
    private boolean asc = true;
    private boolean null_first = false;

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
      this.null_first = nullFirst;
    }

    public final boolean isAscending() {
      return this.asc;
    }

    public final void setDescending() {
      this.asc = false;
    }

    public final boolean isNullFirst() {
      return this.null_first;
    }

    public final void setNullFirst() {
      this.null_first = true;
    }

    public final ColumnReferenceExpr getKey() {
      return this.key;
    }

    public boolean equals(Object obj) {
      if (obj instanceof SortSpec) {
        SortSpec other = (SortSpec) obj;
        return TUtil.checkEquals(key, other.key) &&
            TUtil.checkEquals(asc, other.asc) &&
            TUtil.checkEquals(null_first, other.null_first);
      }
      return false;
    }
  }
}
