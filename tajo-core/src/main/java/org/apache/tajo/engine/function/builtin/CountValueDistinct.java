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
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.Int8Datum;
import org.apache.tajo.engine.function.FunctionContext;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

/**
 * Count(distinct column) function
 */
@Description(
  functionName = "count",
  description = " The number of rows for "
          + "which the supplied expressions are unique and non-NULL.",
  example = "> SELECT count(expr);",
  returnType = Type.INT8,
  paramTypes = {@ParamTypes(paramTypes = {Type.ANY})}
)
public final class CountValueDistinct extends CountRows {

  public CountValueDistinct() {
    super(new Column[] {
        new Column("expr", Type.ANY)
    });
  }

  @Override
  public void eval(FunctionContext context, Tuple params) {
  }

  @Override
  public void merge(FunctionContext context, Tuple part) {
    CountDistinctValueContext distinctContext = (CountDistinctValueContext) context;
    Datum value = part.get(0);

    if (!value.isNull() && (distinctContext.latest == null || (!distinctContext.latest.equals(value)))) {
      distinctContext.latest = value;
      distinctContext.count++;
    }
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    return DatumFactory.createInt8(((CountDistinctValueContext) ctx).count);
  }

  @Override
  public Int8Datum terminate(FunctionContext ctx) {
    return DatumFactory.createInt8(((CountDistinctValueContext) ctx).count);
  }

  @Override
  public FunctionContext newContext() {
    return new CountDistinctValueContext();
  }

  private class CountDistinctValueContext implements FunctionContext {
    long count = 0;
    Datum latest = null;
  }

  @Override
  public CatalogProtos.FunctionType getFunctionType() {
    return CatalogProtos.FunctionType.DISTINCT_AGGREGATION;
  }
}
