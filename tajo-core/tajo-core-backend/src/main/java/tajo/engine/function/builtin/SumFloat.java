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

package tajo.engine.function.builtin;

import tajo.catalog.Column;
import tajo.catalog.function.AggFunction;
import tajo.catalog.function.FunctionContext;
import tajo.catalog.proto.CatalogProtos;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.storage.Tuple;

public class SumFloat extends AggFunction<Datum> {
  public SumFloat() {
    super(new Column[] {
        new Column("val", CatalogProtos.DataType.FLOAT)
    });
  }

  @Override
  public FunctionContext newContext() {
    return new SumContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    ((SumContext)ctx).sum += params.get(0).asFloat();
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    return DatumFactory.createFloat(((SumContext)ctx).sum);
  }

  @Override
  public CatalogProtos.DataType[] getPartialResultType() {
    return new CatalogProtos.DataType[] {DataType.FLOAT};
  }

  @Override
  public Datum terminate(FunctionContext ctx) {
    return DatumFactory.createFloat(((SumContext)ctx).sum);
  }

  private class SumContext implements FunctionContext {
    private float sum;
  }
}
