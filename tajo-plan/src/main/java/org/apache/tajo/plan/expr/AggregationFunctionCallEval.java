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
import org.apache.tajo.plan.function.AggFunction;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.TUtil;

public class AggregationFunctionCallEval extends FunctionEval implements Cloneable {
  @Expose boolean intermediatePhase = false;
  @Expose boolean finalPhase = true;
  @Expose String alias;

  protected AggFunction instance;
  private Tuple params;

  protected AggregationFunctionCallEval(EvalType type, FunctionDesc desc, AggFunction instance, EvalNode[] givenArgs) {
    super(type, desc, givenArgs);
    this.instance = instance;
  }

  public AggregationFunctionCallEval(FunctionDesc desc, AggFunction instance, EvalNode[] givenArgs) {
    super(EvalType.AGG_FUNCTION, desc, givenArgs);
    this.instance = instance;
  }

  public FunctionContext newContext() {
    return instance.newContext();
  }

  public void merge(FunctionContext context, Schema schema, Tuple tuple) {
    if (params == null) {
      this.params = new VTuple(argEvals.length);
    }

    if (argEvals != null) {
      for (int i = 0; i < argEvals.length; i++) {
        params.put(i, argEvals[i].eval(schema, tuple));
      }
    }

    if (!intermediatePhase && !finalPhase) {
      // firstPhase
      instance.eval(context, params);
    } else {
      instance.merge(context, params);
    }
  }

  @Override
  public Datum eval(Schema schema, Tuple tuple) {
    throw new UnsupportedOperationException("Cannot execute eval() of aggregation function");
  }

  public Datum terminate(FunctionContext context) {
    if (!finalPhase) {
      return instance.getPartialResult(context);
    } else {
      return instance.terminate(context);
    }
  }

  @Override
  public DataType getValueType() {
    if (!finalPhase) {
      return instance.getPartialResultType();
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

    clone.finalPhase = finalPhase;
    clone.intermediatePhase = intermediatePhase;
    clone.alias = alias;
    clone.instance = (AggFunction)instance.clone();

    return clone;
  }

  public boolean isIntermediatePhase() {
    return intermediatePhase;
  }

  public void setIntermediatePhase(boolean flag) {
    this.intermediatePhase = flag;
  }

  public void setFinalPhase(boolean flag) {
    this.finalPhase = flag;
  }

  public boolean isFinalPhase() {
    return finalPhase;
  }

  public void setFirstPhase() {
    this.finalPhase = false;
    this.intermediatePhase = false;
  }

  public void setFinalPhase() {
    this.finalPhase = true;
    this.intermediatePhase = false;
  }

  public void setIntermediatePhase() {
    this.finalPhase = false;
    this.intermediatePhase = true;
  }

  public boolean equals(Object obj) {
    if (obj instanceof AggregationFunctionCallEval) {
      AggregationFunctionCallEval other = (AggregationFunctionCallEval) obj;

      boolean eq = super.equals(other);
      eq &= instance.equals(other.instance);
      eq &= intermediatePhase == other.intermediatePhase;
      eq &= finalPhase == other.finalPhase;
      eq &= TUtil.checkEquals(alias, other.alias);
      return eq;
    }

    return false;
  }
}
