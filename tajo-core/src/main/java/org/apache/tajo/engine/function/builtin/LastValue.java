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


public abstract class LastValue extends AggFunction<Datum> {

  public LastValue(Column[] columns) {
    super(columns);
  }

  @Override
  public FunctionContext newContext() {
    return new LastValueContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    LastValueContext lastValueCtx = (LastValueContext) ctx;
    if (!params.isBlankOrNull(0)) {
      lastValueCtx.last = params.asDatum(0);
    }
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    return ((LastValueContext) ctx).last;
  }

  @Override
  public Datum terminate(FunctionContext ctx) {
    if (((LastValueContext) ctx).last == null) {
      return NullDatum.get();
    }
    else {
      return ((LastValueContext) ctx).last;
    }
  }

  private static class LastValueContext implements FunctionContext {
    Datum last = null;
  }
}