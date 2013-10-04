/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.planner;

import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.util.TUtil;

/**
 * A Target contains how to evaluate an expression and its alias name.
 */
public class Target implements Cloneable, GsonObject {
  @Expose private EvalNode expr;
  @Expose private Column column;
  @Expose private String alias = null;

  public Target(EvalNode expr) {
    this.expr = expr;
    this.column = new Column(expr.getName(), expr.getValueType());
  }

  public Target(final EvalNode eval, final String alias) {
    this(eval);
    setAlias(alias);
  }

  public String getCanonicalName() {
    return !hasAlias() ? column.getQualifiedName() : alias;
  }

  public final void setAlias(String alias) {
    this.alias = alias;
    this.column = new Column(alias, expr.getValueType());
  }

  public final String getAlias() {
    return alias;
  }

  public final boolean hasAlias() {
    return alias != null;
  }

  public DataType getDataType() {
    return column.getDataType();
  }

  public EvalNode getEvalTree() {
    return this.expr;
  }

  public Column getColumnSchema() {
    return this.column;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder(expr.toString());
    if(hasAlias()) {
      sb.append(" as ").append(alias);
    }
    return sb.toString();
  }

  public boolean equals(Object obj) {
    if(obj instanceof Target) {
      Target other = (Target) obj;

      boolean b1 = expr.equals(other.expr);
      boolean b2 = column.equals(other.column);
      boolean b3 = TUtil.checkEquals(alias, other.alias);

      return b1 && b2 && b3;
    } else {
      return false;
    }
  }

  public int hashCode() {
    return this.expr.getName().hashCode();
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    Target target = (Target) super.clone();
    target.expr = (EvalNode) expr.clone();
    target.column = (Column) column.clone();
    target.alias = alias != null ? alias : null;

    return target;
  }

  public String toJson() {
    return CoreGsonHelper.toJson(this, Target.class);
  }
}
