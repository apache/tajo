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

public class ColumnReferenceExpr extends Expr {
  private String rel_name;
  private String column_name;

  public ColumnReferenceExpr(String columnName) {
    super(ExprType.Column);
    this.column_name = columnName;
  }

  public void setRelationName(String tableName) {
    this.rel_name = tableName;
  }

  public String getName() {
    return this.column_name;
  }

  public String getRelationName() {
    return this.rel_name;
  }

  public boolean equalsTo(Expr expr) {
    ColumnReferenceExpr another = (ColumnReferenceExpr) expr;
    return column_name.equals(another.column_name) &&
        TUtil.checkEquals(rel_name, another.rel_name);
  }
}
