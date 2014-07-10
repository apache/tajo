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
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.Int8Datum;
import org.apache.tajo.engine.function.FunctionContext;
import org.apache.tajo.engine.function.WindowAggFunc;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

@Description(
  functionName = "row_number",
  description = "the total number of retrieved rows",
  example = "> SELECT row_number() OVER ();",
  returnType = Type.INT8,
  paramTypes = {@ParamTypes(paramTypes = {})}
)
public class RowNumber extends WindowAggFunc<Datum> {

  public RowNumber() {
    super(NoArgs);
  }

  protected RowNumber(Column[] columns) {
    super(columns);
  }

  @Override
  public FunctionContext newContext() {
    return new RowNumberContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    ((RowNumberContext) ctx).count++;
  }

  @Override
  public Int8Datum terminate(FunctionContext ctx) {
    return DatumFactory.createInt8(((RowNumberContext) ctx).count);
  }

  protected class RowNumberContext implements FunctionContext {
    long count = 0;
  }
}
