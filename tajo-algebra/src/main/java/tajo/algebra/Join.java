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

public class Join extends BinaryOperator {
  private JoinType join_type;
  private Expr join_qual;
  private ColumnReferenceExpr [] join_columns;
  private boolean natural = false;

  public Join(JoinType joinType) {
    super(ExprType.Join);
    this.join_type = joinType;
  }

  public JoinType getJoinType() {
    return  this.join_type;
  }

  public boolean hasQual() {
    return this.join_qual != null;
  }

  public Expr getQual() {
    return this.join_qual;
  }

  public void setQual(Expr expr) {
    this.join_qual = expr;
  }

  public boolean hasJoinColumns() {
    return join_columns != null;
  }

  public ColumnReferenceExpr [] getJoinColumns() {
    return join_columns;
  }

  public void setJoinColumns(ColumnReferenceExpr[] columns) {
    join_columns = columns;
  }

  public void setNatural() {
    natural = true;
  }

  public boolean isNatural() {
    return natural;
  }

  boolean equalsTo(Expr expr) {
    Join another = (Join) expr;
    return join_type.equals(another.join_type) &&
        TUtil.checkEquals(join_qual, another.join_qual) &&
        TUtil.checkEquals(join_columns, another.join_columns) &&
        natural == another.natural;
  }

  @Override
  public String toJson() {
    return JsonHelper.toJson(this);
  }
}
