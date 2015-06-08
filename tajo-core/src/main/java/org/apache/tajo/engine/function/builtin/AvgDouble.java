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
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.plan.function.AggFunction;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

import static org.apache.tajo.InternalTypes.AvgDoubleProto;

@Description(
  functionName = "avg",
  description = "The mean of a set of numbers.",
  example = "> SELECT avg(expr);",
  returnType = Type.FLOAT8,
  paramTypes = {@ParamTypes(paramTypes = {Type.FLOAT8})}
)
public class AvgDouble extends AggFunction<Datum> {
  public AvgDouble() {
    super(new Column[] {
        new Column("expr", Type.FLOAT8)
    });
  }

  public AvgDouble(Column[] definedArgs) {
    super(definedArgs);
  }

  public AvgContext newContext() {
    return new AvgContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    AvgContext avgCtx = (AvgContext) ctx;
    if (!params.isBlankOrNull(0)) {
      avgCtx.sum += params.getFloat8(0);
      avgCtx.count++;
    }
  }

  @Override
  public void merge(FunctionContext ctx, Tuple part) {
    AvgContext avgCtx = (AvgContext) ctx;
    if (part.isBlankOrNull(0)) {
      return;
    }
    ProtobufDatum datum = (ProtobufDatum) part.getProtobufDatum(0);
    AvgDoubleProto proto = (AvgDoubleProto) datum.get();
    avgCtx.sum += proto.getSum();
    avgCtx.count += proto.getCount();
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    AvgContext avgCtx = (AvgContext) ctx;
    if (avgCtx.count == 0) {
      return NullDatum.get();
    }
    AvgDoubleProto.Builder builder = AvgDoubleProto.newBuilder();
    builder.setSum(avgCtx.sum);
    builder.setCount(avgCtx.count);
    return new ProtobufDatum(builder.build());
  }

  @Override
  public DataType getPartialResultType() {
    return CatalogUtil.newDataType(Type.PROTOBUF, AvgDoubleProto.class.getName());
  }

  @Override
  public Datum terminate(FunctionContext ctx) {
    AvgContext avgCtx = (AvgContext) ctx;
    if (avgCtx.count == 0) {
      return NullDatum.get();
    }
    return DatumFactory.createFloat8(avgCtx.sum / avgCtx.count);
  }

  protected static class AvgContext implements FunctionContext {
    double sum = 0.0;
    long count = 0;
  }
}