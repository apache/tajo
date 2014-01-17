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

package org.apache.tajo.algebra;

public class GeneralSetFunctionExpr extends FunctionExpr {
  private boolean distinct = false;

  public GeneralSetFunctionExpr(String signature, boolean distinct, Expr param) {
    super(OpType.GeneralSetFunction, signature, new Expr[] {param});
    this.distinct = distinct;
  }

  public boolean isDistinct() {
    return distinct;
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    return hash * 89 * (distinct ? 31 : 23);
  }

  @Override
  public boolean equalsTo(Expr expr) {
    return distinct == ((GeneralSetFunctionExpr)expr).distinct;
  }
}
