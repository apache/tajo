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

import static org.apache.tajo.InternalTypes.StdDevProto;

public abstract class StdDev extends AggFunction<Datum> {

  public StdDev(Column[] definedArgs) {
    super(definedArgs);
  }

  public StdDevContext newContext() {
    return new StdDevContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    StdDevContext StdDevCtx = (StdDevContext) ctx;
    Datum datum = params.get(0);
    if (datum.isNotNull()) {
      double delta = datum.asFloat8() - StdDevCtx.avg;
      StdDevCtx.count++;
      StdDevCtx.avg += delta/StdDevCtx.count;
      StdDevCtx.squareSumOfDiff += delta * (datum.asFloat8() - StdDevCtx.avg);
    }
  }

  @Override
  public void merge(FunctionContext ctx, Tuple part) {
    StdDevContext StdDevCtx = (StdDevContext) ctx;
    Datum d = part.get(0);
    if (d instanceof NullDatum) {
      return;
    }
    ProtobufDatum datum = (ProtobufDatum) d;
    StdDevProto proto = (StdDevProto) datum.get();
    double delta = proto.getAvg() - StdDevCtx.avg;
    StdDevCtx.avg += delta * proto.getCount() / (StdDevCtx.count + proto.getCount());
    StdDevCtx.squareSumOfDiff += proto.getSquareSumOfDiff() + delta * delta * StdDevCtx.count * proto.getCount() / (StdDevCtx.count + proto.getCount());
    StdDevCtx.count += proto.getCount();
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    StdDevContext StdDevCtx = (StdDevContext) ctx;
    if (StdDevCtx.count == 0) {
      return NullDatum.get();
    }
    StdDevProto.Builder builder = StdDevProto.newBuilder();
    builder.setSquareSumOfDiff(StdDevCtx.squareSumOfDiff);
    builder.setAvg(StdDevCtx.avg);
    builder.setCount(StdDevCtx.count);
    return new ProtobufDatum(builder.build());
  }

  @Override
  public DataType getPartialResultType() {
    return CatalogUtil.newDataType(Type.PROTOBUF, StdDevProto.class.getName());
  }

  protected static class StdDevContext implements FunctionContext {
    double squareSumOfDiff = 0.0;
    double avg = 0.0;
    long count = 0;
  }
}