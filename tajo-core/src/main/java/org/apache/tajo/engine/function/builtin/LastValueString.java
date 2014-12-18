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
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.plan.function.AggFunction;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

@Description(
    functionName = "last_value",
    description = "the last value of expr",
    example = "> SELECT last_value(expr);",
    returnType = TajoDataTypes.Type.TEXT,
    paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT})}
)
public class LastValueString  extends AggFunction<Datum> {

  public LastValueString() {
    super(new Column[] {
        new Column("expr", TajoDataTypes.Type.TEXT)
    });
  }

  @Override
  public FunctionContext newContext() {
    return new LastValueContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    LastValueContext lastValueCtx = (LastValueContext) ctx;
    Datum datum = params.get(0);
    if ( datum instanceof NullDatum ) {
      lastValueCtx.isNull = true;
    } else
    {
      lastValueCtx.isNull = false;
    }
    lastValueCtx.lastString = params.get(0).asChars();
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    return DatumFactory.createText(((LastValueContext) ctx).lastString);
  }

  @Override
  public TajoDataTypes.DataType getPartialResultType() {
    return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.TEXT);
  }

  @Override
  public Datum terminate(FunctionContext ctx) {
    if (((LastValueContext) ctx).isNull) {
      return NullDatum.get();
    }
    else {
      return DatumFactory.createText(((LastValueContext) ctx).lastString);
    }
  }

  private class LastValueContext implements FunctionContext {
    String lastString = null;
    boolean isNull = true;
  }
}