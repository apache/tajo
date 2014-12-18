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
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.*;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.plan.function.WindowAggFunc;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

@Description(
    functionName = "first_value",
    description = "the first value of retrieved rows",
    example = "> SELECT first_value(column) OVER ();",
    returnType = Type.TEXT,
    paramTypes = {@ParamTypes(paramTypes = {Type.TEXT})}
)
public class FirstValueString extends WindowAggFunc<Datum> {

  public FirstValueString() {
    super(NoArgs);
  }

  public FirstValueString(Column[] columns) {
    super(columns);
  }

  @Override
  public FunctionContext newContext() {
    return new FirstValueStringContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    FirstValueStringContext firstValueCtx = (FirstValueStringContext)ctx;
    if(firstValueCtx.firstString == null && !(params.get(0) instanceof NullDatum)) {
      firstValueCtx.firstString = params.get(0).asChars();
    }
  }

  @Override
  public Datum terminate(FunctionContext ctx) {
    if (((FirstValueStringContext) ctx).firstString == null) {
      return NullDatum.get();
    }
    else {
      return DatumFactory.createText(((FirstValueStringContext) ctx).firstString);
    }
  }

  protected class FirstValueStringContext implements FunctionContext {
    String firstString = null;
  }

}
