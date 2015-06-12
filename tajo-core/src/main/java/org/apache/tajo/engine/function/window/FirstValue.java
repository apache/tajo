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

public abstract class FirstValue extends WindowAggFunc<Datum> {

  public FirstValue(Column[] columns) {
    super(columns);
  }

  @Override
  public FunctionContext newContext() {
    return new FirstValueContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    FirstValueContext firstValueCtx = (FirstValueContext)ctx;
    if(!firstValueCtx.isSet) {
      firstValueCtx.isSet = true;
      if (!params.isBlankOrNull(0)) {
        firstValueCtx.first = params.asDatum(0);
      }
    }
  }

  @Override
  public Datum terminate(FunctionContext ctx) {
    if (((FirstValueContext) ctx).first == null) {
      return NullDatum.get();
    }
    else {
      return ((FirstValueContext) ctx).first;
    }
  }

  protected static class FirstValueContext implements FunctionContext {
    boolean isSet = false;
    Datum first = null;
  }

}
