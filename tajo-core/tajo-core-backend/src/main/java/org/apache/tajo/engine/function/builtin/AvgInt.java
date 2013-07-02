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
import org.apache.tajo.catalog.function.AggFunction;
import org.apache.tajo.catalog.function.FunctionContext;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.ArrayDatum;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.Float4Datum;
import org.apache.tajo.storage.Tuple;

public class AvgInt extends AggFunction<Float4Datum> {

  public AvgInt() {
    super(new Column[] {
        new Column("val", Type.FLOAT8)
    });
  }

  public AvgContext newContext() {
    return new AvgContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    AvgContext avgCtx = (AvgContext) ctx;
    avgCtx.sum += params.get(0).asInt4();
    avgCtx.count++;
  }

  @Override
  public void merge(FunctionContext ctx, Tuple part) {
    AvgContext avgCtx = (AvgContext) ctx;
    ArrayDatum array = (ArrayDatum) part.get(0);
    avgCtx.sum += array.get(0).asInt8();
    avgCtx.count += array.get(1).asInt8();
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    AvgContext avgCtx = (AvgContext) ctx;
    ArrayDatum part = new ArrayDatum(2);
    part.put(0, DatumFactory.createInt8(avgCtx.sum));
    part.put(1, DatumFactory.createInt8(avgCtx.count));

    return part;
  }

  @Override
  public DataType [] getPartialResultType() {
    return CatalogUtil.newDataTypesWithoutLen(Type.INT8, Type.INT8);
  }

  @Override
  public Float4Datum terminate(FunctionContext ctx) {
    AvgContext avgCtx = (AvgContext) ctx;
    return DatumFactory.createFloat4((float) avgCtx.sum / avgCtx.count);
  }

  private class AvgContext implements FunctionContext {
    long sum;
    long count;
  }
}
