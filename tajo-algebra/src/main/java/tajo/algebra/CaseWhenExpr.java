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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CaseWhenExpr extends Expr {
  private List<WhenExpr> whens = new ArrayList<>();
  private Expr else_result;

  public CaseWhenExpr() {
    super(ExprType.CaseWhen);
  }

  public void addWhen(Expr condition, Expr result) {
    whens.add(new WhenExpr(condition, result));
  }

  public Collection<WhenExpr> getWhens() {
    return this.whens;
  }

  public void setElseResult(Expr else_result) {
    this.else_result = else_result;
  }

  public Object getElseResult() {
    return this.else_result;
  }

  public boolean hasElseResult() {
    return else_result != null;
  }

  @Override
  boolean equalsTo(Expr expr) {
    return false;
  }

  @Override
  public String toJson() {
    return JsonHelper.toJson(this);
  }

  public static class WhenExpr {
    Expr condition;
    Expr result;

    public WhenExpr(Expr condition, Expr result) {
      this.condition = condition;
      this.result = result;
    }

    public Expr getCondition() {
      return this.condition;
    }

    public Expr getResult() {
      return this.result;
    }

    public boolean equals(Object obj) {
      if (obj instanceof WhenExpr) {
        WhenExpr another = (WhenExpr) obj;
      }

      return false;
    }
  }
}
