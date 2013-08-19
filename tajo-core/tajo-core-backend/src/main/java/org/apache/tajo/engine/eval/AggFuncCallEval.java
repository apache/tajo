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

import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.function.AggFunction;
import org.apache.tajo.catalog.function.FunctionContext;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

public class AggFuncCallEval extends FuncEval implements Cloneable {
  @Expose protected AggFunction instance;
  @Expose boolean firstPhase = false;
  private Tuple params;

  public AggFuncCallEval(FunctionDesc desc, AggFunction instance, EvalNode[] givenArgs) {
    super(EvalType.AGG_FUNCTION, desc, givenArgs);
    this.instance = instance;
  }

  @Override
  public EvalContext newContext() {
    return new AggFunctionCtx(argEvals, instance.newContext());
  }

  @Override
  public void eval(EvalContext ctx, Schema schema, Tuple tuple) {
    AggFunctionCtx localCtx = (AggFunctionCtx) ctx;
    if (params == null) {
      this.params = new VTuple(argEvals.length);
    }

    if (argEvals != null) {
      params.clear();

      for (int i = 0; i < argEvals.length; i++) {
        argEvals[i].eval(localCtx.argCtxs[i], schema, tuple);
        params.put(i, argEvals[i].terminate(localCtx.argCtxs[i]));
      }
    }

    if (firstPhase) {
      instance.eval(localCtx.funcCtx, params);
    } else {
      instance.merge(localCtx.funcCtx, params);
    }
  }

  @Override
  public Datum terminate(EvalContext ctx) {
    if (firstPhase) {
      return instance.getPartialResult(((AggFunctionCtx)ctx).funcCtx);
    } else {
      return instance.terminate(((AggFunctionCtx)ctx).funcCtx);
    }
  }

  @Override
  public DataType[] getValueType() {
    if (firstPhase) {
      return instance.getPartialResultType();
    } else {
      return funcDesc.getReturnType();
    }
  }

  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  public void setFirstPhase() {
    this.firstPhase = true;
  }

  protected class AggFunctionCtx extends FuncCallCtx {
    FunctionContext funcCtx;

    AggFunctionCtx(EvalNode [] argEvals, FunctionContext funcCtx) {
      super(argEvals);
      this.funcCtx = funcCtx;
    }
  }
}
