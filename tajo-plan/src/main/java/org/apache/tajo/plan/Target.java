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

package org.apache.tajo.plan;

import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.expr.FieldEval;
import org.apache.tajo.plan.serder.LogicalNodeSerializer;
import org.apache.tajo.plan.serder.PlanGsonHelper;
import org.apache.tajo.plan.serder.PlanProto;
import org.apache.tajo.util.TUtil;

/**
 * A Target contains how to evaluate an expression and its alias name.
 */
public class Target implements Cloneable, GsonObject, ProtoObject<PlanProto.Target> {
  @Expose private EvalNode expr;
  @Expose private Column column;
  @Expose private String alias = null;

  public Target(FieldEval fieldEval) {
    this.expr = fieldEval;
    this.column = fieldEval.getColumnRef();
  }

  public Target(final EvalNode eval, final String alias) {
    this.expr = eval;
    // force lower case
    String normalized = alias;

    // If an expr is a column reference and its alias is equivalent to column name, ignore a given alias.
    if (eval instanceof FieldEval && eval.getName().equals(normalized)) {
      column = ((FieldEval) eval).getColumnRef();
    } else {
      column = new Column(normalized, eval.getValueType());
      setAlias(alias);
    }
  }

  public String getCanonicalName() {
    return !hasAlias() ? column.getQualifiedName() : alias;
  }

  public final void setExpr(EvalNode expr) {
    this.expr = expr;
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

  public <T extends EvalNode> T getEvalTree() {
    return (T) this.expr;
  }

  public Column getNamedColumn() {
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
    target.column = column;
    target.alias = alias != null ? alias : null;

    return target;
  }

  public String toJson() {
    return PlanGsonHelper.toJson(this, Target.class);
  }

  @Override
  public PlanProto.Target getProto() {
    return LogicalNodeSerializer.convertTarget(this);
  }
}
