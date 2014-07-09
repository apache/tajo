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

/**
 * <code>NamedExpr</code> is an expression which can be aliased in a target list.
 *
 * <pre>
 *   SELECT col1 + col2 as a, sum(col2) as b, col3 as c, col4, ... FROM ...
 *          ^^^^^^^^^^^^^^^^  ^^^^^^^^^^^^^^  ^^^^^^^^^  ^^^^
 *               expr1             expr2        expr3    expr4
 * </pre>
 *
 * We define each expression in expr1 - expr4 as a named expression.
 * In database community, each of them is called target or an expression in a select list,
 * Each expression can be explicitly aliased as an given name.
 */
public class NamedExpr extends UnaryOperator {
  @Expose @SerializedName("AliasName")
  private String alias;

  public NamedExpr(Expr expr) {
    super(OpType.Target);
    setChild(expr);
  }

  public NamedExpr(Expr expr, String alias) {
    this(expr);
    setAlias(alias);
  }

  public Expr getExpr() {
    return getChild();
  }

  public boolean hasAlias() {
    return this.alias != null;
  }

  public String getAlias() {
    return this.alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(alias, getChild());
  }

  @Override
  public boolean equalsTo(Expr obj) {
    if (obj instanceof NamedExpr) {
      NamedExpr another = (NamedExpr) obj;
      return TUtil.checkEquals(alias, another.alias);
    }

    return false;
  }

  public String toString() {
    return getChild().toString() + (hasAlias() ? " AS " + alias : "");
  }

  @Override
  public String toJson() {
    return JsonHelper.toJson(this);
  }

  @Override
  public NamedExpr clone() throws CloneNotSupportedException {
    NamedExpr namedExpr = (NamedExpr) super.clone();
    namedExpr.alias = alias;
    return namedExpr;
  }
}
