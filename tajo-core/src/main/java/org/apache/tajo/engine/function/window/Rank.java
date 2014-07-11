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
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.Int8Datum;
import org.apache.tajo.engine.function.FunctionContext;
import org.apache.tajo.engine.function.WindowAggFunc;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

@Description(
    functionName = "rank",
    description = " The number of rows for "
        + "which the supplied expressions are unique and non-NULL.",
    example = "> SELECT rank() OVER (ORDER BY x) FROM ...;",
    returnType = TajoDataTypes.Type.INT8,
    paramTypes = {@ParamTypes(paramTypes = {})}
)
public final class Rank extends WindowAggFunc {

  public Rank() {
    super(new Column[] {
        new Column("expr", TajoDataTypes.Type.ANY)
    });
  }

  public static boolean checkIfDistinctValue(RankContext context, Tuple params) {
    for (int i = 0; i < context.latest.length; i++) {
      if (!context.latest[i].equalsTo(params.get(i)).isTrue()) {
        return true;
      }
    }

    return false;
  }

  @Override
  public void eval(FunctionContext context, Tuple params) {
    RankContext ctx = (RankContext) context;

    if ((ctx.latest == null || checkIfDistinctValue(ctx, params))) {
      ctx.rank = ctx.accumulatedCount;
      ctx.latest = params.getValues().clone();
    }
    ctx.accumulatedCount++;
  }

  @Override
  public Int8Datum terminate(FunctionContext ctx) {
    return DatumFactory.createInt8(((RankContext) ctx).rank);
  }

  @Override
  public FunctionContext newContext() {
    return new RankContext();
  }

  private class RankContext implements FunctionContext {
    long rank = 0;
    long accumulatedCount = 1;
    Datum [] latest = null;
  }

  @Override
  public CatalogProtos.FunctionType getFunctionType() {
    return CatalogProtos.FunctionType.WINDOW;
  }
}
