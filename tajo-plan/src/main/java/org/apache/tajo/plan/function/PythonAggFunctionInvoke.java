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

package org.apache.tajo.plan.function;

import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.plan.function.python.PythonScriptEngine;
import org.apache.tajo.plan.serder.EvalNodeSerializer;
import org.apache.tajo.plan.serder.PlanProto;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;

public class PythonAggFunctionInvoke extends AggFunctionInvoke implements Cloneable {

  private transient PythonScriptEngine scriptEngine;

  public PythonAggFunctionInvoke(FunctionDesc functionDesc) {
    super(functionDesc);
  }

  @Override
  public void init(FunctionInvokeContext context) throws IOException {
    this.scriptEngine = (PythonScriptEngine) context.getScriptEngine();
  }

  @Override
  public FunctionContext newContext() {
    // nothing to do
    return null;
  }

  @Override
  public void eval(FunctionContext context, Tuple params) {
    scriptEngine.callAggFunc(true, params);
  }

  @Override
  public void merge(FunctionContext context, Tuple params) {
    scriptEngine.callAggFunc(false, params);
  }

  @Override
  public Datum getPartialResult(FunctionContext context) {
    // TODO: get tuples from script engine
    Schema intermSchema = scriptEngine.getIntermSchema();
    Tuple intermResult = scriptEngine.getIntermResult();
    PlanProto.NamedTuple.Builder builder = PlanProto.NamedTuple.newBuilder();
    for (int i = 0; i < intermResult.size(); i++) {
      PlanProto.NamedDatum.Builder datumBuilder = PlanProto.NamedDatum.newBuilder();
      datumBuilder.setName(intermSchema.getColumn(i).getSimpleName());
      datumBuilder.setVal(EvalNodeSerializer.serialize(intermResult.get(i)));
      builder.addDatums(datumBuilder.build());
    }
    return new ProtobufDatum(builder.build());
  }

  @Override
  public TajoDataTypes.DataType getPartialResultType() {
    return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.PROTOBUF);
  }

  @Override
  public Datum terminate(FunctionContext context) {
    return scriptEngine.getFinalResult();
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    // nothing to do
    return super.clone();
  }
}
