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

import tajo.catalog.CatalogUtil;
import tajo.catalog.Column;
import tajo.catalog.function.AggFunction;
import tajo.catalog.function.FunctionContext;
import tajo.common.TajoDataTypes.DataType;
import tajo.common.TajoDataTypes.Type;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.datum.Int8Datum;
import tajo.storage.Tuple;

public class CountRows extends AggFunction<Datum> {

  public CountRows() {
    super(NoArgs);
  }

  protected CountRows(Column[] columns) {
    super(columns);
  }

  @Override
  public FunctionContext newContext() {
    return new CountRowContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    ((CountRowContext) ctx).count++;
  }

  @Override
  public void merge(FunctionContext ctx, Tuple part) {
    ((CountRowContext) ctx).count += part.get(0).asInt8();
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    return DatumFactory.createInt8(((CountRowContext) ctx).count);
  }

  @Override
  public DataType[] getPartialResultType() {
    return CatalogUtil.newDataTypesWithoutLen(Type.INT8);
  }

  @Override
  public Int8Datum terminate(FunctionContext ctx) {
    return DatumFactory.createInt8(((CountRowContext) ctx).count);
  }

  protected class CountRowContext implements FunctionContext {
    long count;
  }
}
