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


public abstract class NthValue extends AggFunction<Datum> {

  public NthValue(Column[] columns) {
    super(columns);
  }

  @Override
  public FunctionContext newContext() {
    return new NthValueContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    NthValueContext nThValueCtx = (NthValueContext) ctx;
    Datum datum = params.get(1);
    nThValueCtx.rowCount++;
    if (nThValueCtx.rowCount == datum.asInt8()) {
      nThValueCtx.nThValue = params.get(0);
    }
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    return ((NthValueContext) ctx).nThValue;
  }

  @Override
  public Datum terminate(FunctionContext ctx) {
    if (((NthValueContext) ctx).nThValue == null) {
      return NullDatum.get();
    }
    else {
      return ((NthValueContext) ctx).nThValue;
    }
  }

  protected static class NthValueContext implements FunctionContext {
    Datum nThValue = null;
    long rowCount = 0;
  }
}