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

public class LikeExpr extends BinaryOperator {
  private boolean not;
  private ColumnReferenceExpr column_ref;
  private Expr pattern;

  public LikeExpr(boolean not, ColumnReferenceExpr columnReferenceExpr, Expr pattern) {
    super(ExprType.Like);
    this.not = not;
    this.column_ref = columnReferenceExpr;
    this.pattern = pattern;
  }

  public boolean isNot() {
    return not;
  }

  public ColumnReferenceExpr getColumnRef() {
    return this.column_ref;
  }

  public Expr getPattern() {
    return this.pattern;
  }

  boolean equalsTo(Expr expr) {
    LikeExpr another = (LikeExpr) expr;
    return not == another.not &&
        column_ref.equals(another.column_ref) &&
        pattern.equals(another.pattern);
  }
}
