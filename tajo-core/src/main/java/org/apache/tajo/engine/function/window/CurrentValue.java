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

package org.apache.tajo.engine.function.window;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.datum.*;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.plan.function.WindowAggFunc;
import org.apache.tajo.storage.Tuple;

public abstract class CurrentValue extends WindowAggFunc<Datum> {

  public CurrentValue(Column[] columns) {
    super(columns);
  }

  @Override
  public FunctionContext newContext() {
    return new CurrentValueContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    CurrentValueContext currentValueCtx = (CurrentValueContext)ctx;
      if (params.get(0).isNotNull()) {
        currentValueCtx.current = params.get(0);
      } else {
        currentValueCtx.current = NullDatum.get();
      }
  }

  @Override
  public Datum terminate(FunctionContext ctx) {
    return ((CurrentValueContext) ctx).current;
  }

  protected static class CurrentValueContext implements FunctionContext {
    Datum current = null;
  }

}
