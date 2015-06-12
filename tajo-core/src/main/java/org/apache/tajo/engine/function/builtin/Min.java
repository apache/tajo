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

package org.apache.tajo.engine.function.builtin;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.plan.function.AggFunction;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.storage.Tuple;

public abstract class Min extends AggFunction<Datum> {
  public Min(Column[] definedArgs) {
    super(definedArgs);
  }

  @Override
  public FunctionContext newContext() {
    return new MinContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    MinContext minCtx = (MinContext) ctx;
    if (!params.isBlankOrNull(0)) {
      Datum datum = params.asDatum(0);
      if (minCtx.min == null || minCtx.min.compareTo(datum) > 0) {
        minCtx.min = datum;
      }
    }
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    Datum min = ((MinContext)ctx).min;

    if (min == null) {
      return NullDatum.get();
    }
    else {
      return min;
    }
  }

  @Override
  public Datum terminate(FunctionContext ctx) {
    Datum min = ((MinContext)ctx).min;

    if (min == null) {
      return NullDatum.get();
    }
    else {
      return min;
    }
  }

  private static class MinContext implements FunctionContext {
    Datum min = null;
  }
}
