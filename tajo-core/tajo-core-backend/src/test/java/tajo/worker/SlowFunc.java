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

package tajo.worker;

import org.mortbay.log.Log;
import tajo.catalog.Column;
import tajo.catalog.function.AggFunction;
import tajo.catalog.function.FunctionContext;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.datum.Datum;
import tajo.storage.Tuple;

public class SlowFunc extends AggFunction {
  private Datum param;

  public SlowFunc() {
    super(new Column[] { new Column("name", DataType.STRING) });
  }

  @Override
  public FunctionContext newContext() {
    return null;
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    param = params.get(0);
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    return null;
  }

  @Override
  public DataType[] getPartialResultType() {
    return new DataType[] {DataType.STRING};
  }

  @Override
  public Datum terminate(FunctionContext ctx) {
    try {
      Thread.sleep(1000);
      Log.info("Sleepy... z...z...z");
    } catch (InterruptedException ie) {
    }
    return param;
  }
}
