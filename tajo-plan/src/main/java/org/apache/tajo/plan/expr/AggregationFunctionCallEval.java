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

import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.plan.function.AggFunctionInvoke;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.plan.function.FunctionInvokeContext;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.TUtil;

import java.io.IOException;

public class AggregationFunctionCallEval extends FunctionEval implements Cloneable {

  // Both firstPhase and lastPhase flags should be true before global planning.
  @Expose private boolean firstPhase = true;
  @Expose private boolean lastPhase = true;
  @Expose private String alias;

  @Expose protected FunctionInvokeContext invokeContext;
  protected transient AggFunctionInvoke functionInvoke;

  protected AggregationFunctionCallEval(EvalType type, FunctionDesc desc, EvalNode[] givenArgs) {
    super(type, desc, givenArgs);
    this.invokeContext = new FunctionInvokeContext(null, getParamType());
    try {
      this.functionInvoke = AggFunctionInvoke.newInstance(funcDesc);
    } catch (InternalException e) {
      throw new RuntimeException(e);
    }
  }

  public AggregationFunctionCallEval(FunctionDesc desc, EvalNode[] givenArgs) {
    this(EvalType.AGG_FUNCTION, desc, givenArgs);
  }

  public FunctionContext newContext() {
    return functionInvoke.newContext();
  }

  @Override
  public EvalNode bind(EvalContext evalContext, Schema schema) {
    super.bind(evalContext, schema);

    try {
      if (evalContext != null && evalContext.hasScriptEngine(this)) {
        this.invokeContext.setScriptEngine(evalContext.getScriptEngine(this));
        this.invokeContext.getScriptEngine().setFirstPhase(firstPhase);
        this.invokeContext.getScriptEngine().setLastPhase(lastPhase);
      }
      this.functionInvoke.init(invokeContext);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return this;
  }

  public void merge(FunctionContext context, Tuple tuple) {
    if (!isBound) {
      throw new IllegalStateException("bind() must be called before merge()");
    }
    mergeParam(context, evalParams(tuple));
  }

  protected void mergeParam(FunctionContext context, Tuple params) {
    if (firstPhase) {
      // firstPhase
      functionInvoke.eval(context, params);
    } else {
      functionInvoke.merge(context, params);
    }
  }

  @Override
  public <T extends Datum> T eval(Tuple tuple) {
    throw new IllegalStateException("Cannot execute aggregation function in generic expression");
  }

  public Datum terminate(FunctionContext context) {
    if (!isBound) {
      throw new IllegalStateException("bind() must be called before terminate()");
    }
    if (!lastPhase) {
      return functionInvoke.getPartialResult(context);
    } else {
      return functionInvoke.terminate(context);
    }
  }

  @Override
  public DataType getValueType() {
    if (!lastPhase) {
      return functionInvoke.getPartialResultType();
    } else {
      return funcDesc.getReturnType();
    }
  }

  public boolean hasAlias() {
    return this.alias != null;
  }

  public void setAlias(String alias) { this.alias = alias; }

  public String getAlias() { return  this.alias; }

  public Object clone() throws CloneNotSupportedException {
    AggregationFunctionCallEval clone = (AggregationFunctionCallEval)super.clone();

    clone.lastPhase = lastPhase;
    clone.firstPhase = firstPhase;
    clone.alias = alias;
    clone.invokeContext = (FunctionInvokeContext) invokeContext.clone();
    if (functionInvoke != null) {
      clone.functionInvoke = functionInvoke;
    }

    return clone;
  }

  public boolean isFirstPhase() {
    return firstPhase;
  }

  public boolean isLastPhase() {
    return lastPhase;
  }

  public void setFirstPhase() {
    this.firstPhase = true;
    this.lastPhase = false;
  }

  public void setLastPhase() {
    this.firstPhase = false;
    this.lastPhase = true;
  }

  public void setFirstAndLastPhase() {
    this.lastPhase = this.firstPhase = true;
  }

  public void setIntermediatePhase() {
    this.firstPhase = false;
    this.lastPhase = false;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((alias == null) ? 0 : alias.hashCode());
    result = prime * result + (lastPhase ? 1231 : 1237);
    result = prime * result + (firstPhase ? 1249 : 1259);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof AggregationFunctionCallEval) {
      AggregationFunctionCallEval other = (AggregationFunctionCallEval) obj;

      boolean eq = super.equals(other);
      eq &= firstPhase == other.firstPhase;
      eq &= lastPhase == other.lastPhase;
      eq &= TUtil.checkEquals(alias, other.alias);
      return eq;
    }

    return false;
  }
}
