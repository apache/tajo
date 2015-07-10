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

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.plan.function.WindowAggFunc;
import org.apache.tajo.storage.Tuple;

public abstract class Lag extends WindowAggFunc<Datum> {

  public Lag(Column[] columns) {
    super(columns);
  }

  @Override
  public FunctionContext newContext() {
    return new LagContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    LagContext lagCtx = (LagContext)ctx;
    if(lagCtx.lagBuffer == null) {
      int lagNum = 0;
      if (params.size() == 1) {
        lagNum = 1;
      } else {
        lagNum = params.getInt4(1);
      }
      lagCtx.lagBuffer = new CircularFifoBuffer(lagNum+1);
    }

    if (!params.isBlankOrNull(0)) {
      lagCtx.lagBuffer.add(params.asDatum(0));
    } else {
      lagCtx.lagBuffer.add(NullDatum.get());
    }

    if (lagCtx.defaultDatum == null) {
     if (params.size() == 3) {
       lagCtx.defaultDatum = params.asDatum(2);
     } else {
       lagCtx.defaultDatum = NullDatum.get();
     }
    }
  }

  @Override
  public Datum terminate(FunctionContext ctx) {
    LagContext lagCtx = (LagContext)ctx;
    if(lagCtx.lagBuffer.isFull()) {
      return (Datum)(lagCtx.lagBuffer.get());
    } else {
      return lagCtx.defaultDatum;
    }
  }

  protected static class LagContext implements FunctionContext {
    CircularFifoBuffer lagBuffer = null;
    Datum defaultDatum = null;
  }
}
