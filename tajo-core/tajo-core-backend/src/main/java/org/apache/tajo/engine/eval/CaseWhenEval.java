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

package org.apache.tajo.engine.eval;

import com.google.common.collect.Lists;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.TUtil;

import java.util.List;

public class CaseWhenEval extends EvalNode implements GsonObject {
  @Expose private List<IfThenEval> whens = Lists.newArrayList();
  @Expose private EvalNode elseResult;

  public CaseWhenEval() {
    super(EvalType.CASE);
  }

  public void addWhen(EvalNode condition, EvalNode result) {
    whens.add(new IfThenEval(condition, result));
  }

  public List<IfThenEval> getIfThenEvals() {
    return whens;
  }

  public boolean hasElse() {
    return this.elseResult != null;
  }

  public EvalNode getElse() {
    return elseResult;
  }

  public void setElseResult(EvalNode elseResult) {
    this.elseResult = elseResult;
  }

  @Override
  public EvalContext newContext() {
    return new CaseContext(whens, elseResult != null ? elseResult.newContext() : null);
  }

  @Override
  public DataType getValueType() {
    return whens.get(0).getResultExpr().getValueType();
  }

  @Override
  public String getName() {
    return "?";
  }

  public void eval(EvalContext ctx, Schema schema, Tuple tuple) {
    CaseContext caseCtx = (CaseContext) ctx;
    for (int i = 0; i < whens.size(); i++) {
      whens.get(i).eval(caseCtx.contexts[i], schema, tuple);
    }

    if (elseResult != null) { // without else clause
      elseResult.eval(caseCtx.elseCtx, schema, tuple);
    }
  }

  @Override
  public Datum terminate(EvalContext ctx) {
    CaseContext caseCtx = (CaseContext) ctx;
    for (int i = 0; i < whens.size(); i++) {
      if (whens.get(i).terminate(caseCtx.contexts[i]).asBool()) {
        return whens.get(i).getThenResult(caseCtx.contexts[i]);
      }
    }
    if (elseResult != null) { // without else clause
      return elseResult.terminate(caseCtx.elseCtx);
    } else {
      return DatumFactory.createNullDatum();
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("CASE ");
    for (IfThenEval when : whens) {
     sb.append(when).append(" ");
    }

    sb.append("ELSE ").append(elseResult).append(" END");

    return sb.toString();
  }

  @Override
  public void preOrder(EvalNodeVisitor visitor) {
    visitor.visit(this);
    for (IfThenEval when : whens) {
      when.preOrder(visitor);
    }
    if (elseResult != null) { // without else clause
      elseResult.preOrder(visitor);
    }
  }

  @Override
  public void postOrder(EvalNodeVisitor visitor) {
    for (IfThenEval when : whens) {
      when.postOrder(visitor);
    }
    if (elseResult != null) { // without else clause
      elseResult.postOrder(visitor);
    }
    visitor.visit(this);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof CaseWhenEval) {
      CaseWhenEval other = (CaseWhenEval) obj;

      for (int i = 0; i < other.whens.size(); i++) {
        if (!whens.get(i).equals(other.whens.get(i))) {
          return false;
        }
      }
      return TUtil.checkEquals(elseResult, other.elseResult);
    } else {
      return false;
    }
  }

  public static class IfThenEval extends EvalNode implements GsonObject {
    @Expose private EvalNode condition;
    @Expose private EvalNode result;

    public IfThenEval(EvalNode condition, EvalNode result) {
      super(EvalType.IF_THEN);
      this.condition = condition;
      this.result = result;
    }

    @Override
    public EvalContext newContext() {
      return new WhenContext(condition.newContext(), result.newContext());
    }

    @Override
    public DataType getValueType() {
      return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.BOOLEAN);
    }

    @Override
    public String getName() {
      return "when?";
    }

    public void eval(EvalContext ctx, Schema schema, Tuple tuple) {
      condition.eval(((WhenContext) ctx).condCtx, schema, tuple);
      result.eval(((WhenContext) ctx).resultCtx, schema, tuple);
    }

    @Override
    public Datum terminate(EvalContext ctx) {
      return condition.terminate(((WhenContext) ctx).condCtx);
    }

    public EvalNode getConditionExpr() {
      return this.condition;
    }

    public EvalNode getResultExpr() {
      return this.result;
    }

    @Override
    public boolean equals(Object object) {
      if (object instanceof IfThenEval) {
        IfThenEval other = (IfThenEval) object;
        return condition.equals(other.condition) && result.equals(other.result);
      } else {
        return false;
      }
    }

    @Override
    public String toString() {
      return "WHEN " + condition + " THEN " + result;
    }

    @Override
    public String toJson() {
      return CoreGsonHelper.toJson(IfThenEval.this, IfThenEval.class);
    }

    private class WhenContext implements EvalContext {
      EvalContext condCtx;
      EvalContext resultCtx;

      public WhenContext(EvalContext condCtx, EvalContext resultCtx) {
        this.condCtx = condCtx;
        this.resultCtx = resultCtx;
      }
    }

    public Datum getThenResult(EvalContext ctx) {
      return result.terminate(((WhenContext) ctx).resultCtx);
    }

    @Override
    public void preOrder(EvalNodeVisitor visitor) {
      visitor.visit(this);
      condition.preOrder(visitor);
      result.preOrder(visitor);
    }

    @Override
    public void postOrder(EvalNodeVisitor visitor) {
      condition.postOrder(visitor);
      result.postOrder(visitor);
      visitor.visit(this);
    }
  }

  private class CaseContext implements EvalContext {
    EvalContext [] contexts;
    EvalContext elseCtx;

    CaseContext(List<IfThenEval> whens, EvalContext elseCtx) {
      contexts = new EvalContext[whens.size()];
      for (int i = 0; i < whens.size(); i++) {
        contexts[i] = whens.get(i).newContext();
      }
      this.elseCtx = elseCtx;
    }
  }
}
