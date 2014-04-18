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
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.Float8Datum;
import org.apache.tajo.engine.function.AggFunction;
import org.apache.tajo.engine.function.FunctionContext;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

@Description(
  functionName = "max",
  description = "the maximum value of expr",
  example = "> SELECT max(expr);",
  returnType = Type.FLOAT8,
  paramTypes = {@ParamTypes(paramTypes = {Type.FLOAT8})}
)
public class MaxDouble extends AggFunction<Float8Datum> {

  public MaxDouble() {
    super(new Column[] {
        new Column("expr", Type.FLOAT8)
    });
  }

  @Override
  public FunctionContext newContext() {
    return new MaxContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    MaxContext maxCtx = (MaxContext) ctx;
    maxCtx.max = Math.max(maxCtx.max, params.get(0).asFloat8());
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    return DatumFactory.createFloat8(((MaxContext) ctx).max);
  }

  @Override
  public DataType getPartialResultType() {
    return CatalogUtil.newSimpleDataType(Type.FLOAT8);
  }

  @Override
  public Float8Datum terminate(FunctionContext ctx) {
    return DatumFactory.createFloat8(((MaxContext) ctx).max);
  }

  private class MaxContext implements FunctionContext {
    double max;
  }
}
