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

public class TableSubQuery extends Relation {
  private Expr subquery;

  public TableSubQuery(String relName, Expr subquery) {
    super(ExprType.TableSubQuery, relName);
    this.subquery = subquery;
  }

  public Expr getSubQuery() {
    return subquery;
  }

  @Override
  boolean equalsTo(Expr expr) {
    return subquery.equals(subquery);
  }

  public String toJson() {
    return JsonHelper.toJson(this);
  }
}
