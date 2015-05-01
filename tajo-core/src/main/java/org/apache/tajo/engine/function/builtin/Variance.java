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
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.plan.function.AggFunction;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.storage.Tuple;

import static org.apache.tajo.InternalTypes.VarianceProto;

public abstract class Variance extends AggFunction<Datum> {

  public Variance(Column[] definedArgs) {
    super(definedArgs);
  }

  public VarianceContext newContext() {
    return new VarianceContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    VarianceContext varianceCtx = (VarianceContext) ctx;
    Datum datum = params.get(0);
    if (datum.isNotNull()) {
      double delta = datum.asFloat8() - varianceCtx.avg;
      varianceCtx.count++;
      varianceCtx.avg += delta/varianceCtx.count;
      varianceCtx.squareSumOfDiff += delta * (datum.asFloat8() - varianceCtx.avg);
    }
  }

  @Override
  public void merge(FunctionContext ctx, Tuple part) {
    VarianceContext varianceCtx = (VarianceContext) ctx;
    Datum d = part.get(0);
    if (d instanceof NullDatum) {
      return;
    }
    ProtobufDatum datum = (ProtobufDatum) d;
    VarianceProto proto = (VarianceProto) datum.get();
    double delta = proto.getAvg() - varianceCtx.avg;
    varianceCtx.avg += delta * proto.getCount() / (varianceCtx.count + proto.getCount());
    varianceCtx.squareSumOfDiff += proto.getSquareSumOfDiff() + delta * delta * varianceCtx.count * proto.getCount() / (varianceCtx.count + proto.getCount());
    varianceCtx.count += proto.getCount();
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    VarianceContext varianceCtx = (VarianceContext) ctx;
    if (varianceCtx.count == 0) {
      return NullDatum.get();
    }
    VarianceProto.Builder builder = VarianceProto.newBuilder();
    builder.setSquareSumOfDiff(varianceCtx.squareSumOfDiff);
    builder.setAvg(varianceCtx.avg);
    builder.setCount(varianceCtx.count);
    return new ProtobufDatum(builder.build());
  }

  @Override
  public DataType getPartialResultType() {
    return CatalogUtil.newDataType(Type.PROTOBUF, VarianceProto.class.getName());
  }

  protected static class VarianceContext implements FunctionContext {
    double squareSumOfDiff = 0.0;
    double avg = 0.0;
    long count = 0;
  }
}