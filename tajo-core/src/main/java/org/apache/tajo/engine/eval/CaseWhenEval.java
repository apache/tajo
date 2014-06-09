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
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.json.GsonObject;
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
  public DataType getValueType() {
    return whens.get(0).getResult().getValueType();
  }

  @Override
  public String getName() {
    return "?";
  }

  public Datum eval(Schema schema, Tuple tuple) {
    for (int i = 0; i < whens.size(); i++) {
      if (whens.get(i).checkIfCondition(schema, tuple)) {
        return whens.get(i).eval(schema, tuple);
      }
    }

    if (elseResult != null) { // without else clause
      return elseResult.eval(schema, tuple);
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
    public String getName() {
      return "when?";
    }

    public boolean checkIfCondition(Schema schema, Tuple tuple) {
      return condition.eval(schema, tuple).isTrue();
    }

    public Datum eval(Schema schema, Tuple tuple) {
      return result.eval(schema, tuple);
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
