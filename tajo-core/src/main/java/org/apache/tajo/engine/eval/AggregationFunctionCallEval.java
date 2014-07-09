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
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.function.AggFunction;
import org.apache.tajo.engine.function.FunctionContext;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

public class AggregationFunctionCallEval extends FunctionEval implements Cloneable {
  @Expose protected AggFunction instance;
  @Expose boolean firstPhase = false;
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

    if (firstPhase) {
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
    if (firstPhase) {
      return instance.getPartialResult(context);
    } else {
      return instance.terminate(context);
    }
  }

  @Override
  public DataType getValueType() {
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
}
