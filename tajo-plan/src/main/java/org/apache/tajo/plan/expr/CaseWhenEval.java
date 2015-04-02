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

package org.apache.tajo.plan.expr;

import com.google.common.collect.Lists;
import com.google.gson.annotations.Expose;

import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.plan.serder.PlanGsonHelper;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.TUtil;

import java.util.ArrayList;
import java.util.List;

public class CaseWhenEval extends EvalNode implements GsonObject {
  @Expose private List<IfThenEval> whens = Lists.newArrayList();
  @Expose private EvalNode elseResult;

  public CaseWhenEval() {
    super(EvalType.CASE);
  }

  public void addIfCond(IfThenEval ifCond) {
    whens.add(ifCond);
  }

  public void addIfCond(EvalNode condition, EvalNode result) {
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
  public DataType getValueType() {
    // Find not null type
    for (IfThenEval eachWhen: whens) {
      if (eachWhen.getResult().getValueType().getType() != Type.NULL_TYPE) {
        return eachWhen.getResult().getValueType();
      }
    }

    if (elseResult != null) { // without else clause
      return elseResult.getValueType();
    }

    return NullDatum.getDataType();
  }

  @Override
  public int childNum() {
    return whens.size() + (elseResult != null ? 1 : 0);
  }

  @Override
  public EvalNode getChild(int idx) {
    if (idx < whens.size()) {
      return whens.get(idx);
    } else if (idx == whens.size()) {
      return elseResult;
    } else {
      throw new ArrayIndexOutOfBoundsException(idx);
    }
  }

  @Override
  public String getName() {
    return "?";
  }

  @Override
  @SuppressWarnings("unchecked")
  public Datum eval(Tuple tuple) {
    for (IfThenEval eval : whens) {
      if (eval.checkIfCondition(tuple)) {
        return eval.eval(tuple);
      }
    }

    if (elseResult != null) { // without else clause
      return elseResult.eval(tuple);
    }

    return NullDatum.get();
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
  public Object clone() throws CloneNotSupportedException {
    CaseWhenEval caseWhenEval = (CaseWhenEval) super.clone();
    caseWhenEval.whens = new ArrayList<IfThenEval>();
    for (IfThenEval ifThenEval : whens) {
      caseWhenEval.whens.add((IfThenEval) ifThenEval.clone());
    }
    caseWhenEval.elseResult = elseResult;
    return caseWhenEval;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((elseResult == null) ? 0 : elseResult.hashCode());
    result = prime * result + ((whens == null) ? 0 : whens.hashCode());
    return result;
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
    public DataType getValueType() {
      return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.BOOLEAN);
    }

    @Override
    public int childNum() {
      return 2;
    }

    @Override
    public EvalNode getChild(int idx) {
      if (idx == 0) {
        return condition;
      } else if (idx == 1) {
        return result;
      } else {
        throw new ArrayIndexOutOfBoundsException(idx);
      }
    }

    @Override
    public String getName() {
      return "when?";
    }

    public boolean checkIfCondition(Tuple tuple) {
      return condition.eval(tuple).isTrue();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Datum eval(Tuple tuple) {
      return result.eval(tuple);
    }

    public void setCondition(EvalNode condition) {
      this.condition = condition;
    }

    public EvalNode getCondition() {
      return this.condition;
    }

    public void setResult(EvalNode result) {
      this.result = result;
    }

    public EvalNode getResult() {
      return this.result;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((condition == null) ? 0 : condition.hashCode());
      result = prime * result + ((this.result == null) ? 0 : this.result.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof IfThenEval) {
        IfThenEval other = (IfThenEval) obj;
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
      return PlanGsonHelper.toJson(IfThenEval.this, IfThenEval.class);
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

    @Override
    public Object clone() throws CloneNotSupportedException {
      IfThenEval ifThenEval = (IfThenEval) super.clone();
      ifThenEval.condition = (EvalNode)condition.clone();
      ifThenEval.result = (EvalNode)result.clone();
      return ifThenEval;
    }
  }
}
