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

import com.google.common.base.Preconditions;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

/**
 * Describes SQL Standard set function (e.g., sum, min, max, avg, and count)
 */
public class GeneralSetFunctionExpr extends FunctionExpr {
  @Expose @SerializedName("IsDistinct")
  private boolean distinct = false;

  /**
   *
   * @param type Function type which must be one of GeneralSetFunction or CountRowFunction
   * @param signature Function name
   * @param distinct True if this function is a distinct aggregation function
   * @param params An array of function parameters
   */
  protected GeneralSetFunctionExpr(OpType type, String signature, boolean distinct, Expr [] params) {
    super(type, signature, params);
    this.distinct = distinct;
  }

  /**
   *
   * @param signature Function name
   * @param distinct True if this function is a distinct aggregation function
   * @param params Function parameters
   */
  public GeneralSetFunctionExpr(String signature, boolean distinct, Expr[] params) {
    this(OpType.GeneralSetFunction, signature, distinct, params);
  }

  /**
   *
   * @return True if this function is a distinct aggregation function. Otherwise, it returns False.
   */
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

  @Override
  public Object clone() throws CloneNotSupportedException {
    GeneralSetFunctionExpr newSetFunc = (GeneralSetFunctionExpr) super.clone();
    newSetFunc.distinct = distinct;
    return newSetFunc;
  }
}
