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

import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.Int4Datum;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.plan.function.AggFunction;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.storage.Tuple;

@Description(
    functionName = "ntile",
    description = "integer ranging from 1 to the argument value",
    example = "> SELECT ntile(expr);",
    returnType = TajoDataTypes.Type.INT4,
    paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.INT4})}
)
public class NtileInt extends AggFunction<Datum> {
  public NtileInt() {
    super(new Column[] {
        new Column("expr", TajoDataTypes.Type.INT4)
    });
  }
  @Override
  public FunctionContext newContext() {
    return new NtileContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    NtileContext ntileCtx = (NtileContext) ctx;
    if (ntileCtx.params == -1)
      ntileCtx.params = params.get(0).asInt4();
    ntileCtx.totCont++;
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    Datum d = null;
    return d;
  }

  @Override
  public TajoDataTypes.DataType getPartialResultType() {
    return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INT4);
  }

  @Override
  public Int4Datum terminate(FunctionContext ctx) {
    NtileContext ntileCtx = (NtileContext)ctx;
    if (ntileCtx.totCont == 0) {
      return DatumFactory.createInt4(0);
    }
    if(ntileCtx.overRangNum==-1) {
      ntileCtx.overRangNum=ntileCtx.totCont%ntileCtx.params;
      ntileCtx.tileNum = 1;
    }

    int range = ntileCtx.totCont/ntileCtx.params;
    if (ntileCtx.rangeCnt < range) {
      ntileCtx.rangeCnt++;
      return DatumFactory.createInt4(ntileCtx.tileNum);
    }
    else if (ntileCtx.rangeCnt == range && ntileCtx.overRangNum > 0 ) {
      ntileCtx.overRangNum--;
      ntileCtx.rangeCnt = 0;
      return DatumFactory.createInt4(ntileCtx.tileNum++);
    }
    else if (ntileCtx.rangeCnt == range && ntileCtx.overRangNum == 0) {
      ntileCtx.rangeCnt = 1;
      return DatumFactory.createInt4(++ntileCtx.tileNum);
    }
    return DatumFactory.createInt4(0);
  }

  protected static class NtileContext implements FunctionContext {
    int totCont = 0;
    int params = -1;
    int tileNum = 0;
    int overRangNum = -1;
    int rangeCnt = 0;
  }
}