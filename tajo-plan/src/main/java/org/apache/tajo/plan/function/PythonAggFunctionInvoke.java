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
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.plan.function.python.PythonScriptEngine;
import org.apache.tajo.plan.serder.EvalNodeDeserializer;
import org.apache.tajo.plan.serder.EvalNodeSerializer;
import org.apache.tajo.plan.serder.PlanProto;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

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
    scriptEngine.callAggFunc(params);
  }

  @Override
  public void merge(FunctionContext context, Tuple params) {
    if (params.get(0) instanceof NullDatum) {
      return;
    }
    ProtobufDatum protobufDatum = (ProtobufDatum) params.get(0);
    PlanProto.Tuple namedTuple = (PlanProto.Tuple) protobufDatum.get();
    Tuple input = new VTuple(namedTuple.getDatumsCount());
    for (int i = 0; i < namedTuple.getDatumsCount(); i++) {
      input.put(i, EvalNodeDeserializer.deserialize(namedTuple.getDatums(i)));
    }

    scriptEngine.callAggFunc(input);
  }

  @Override
  public Datum getPartialResult(FunctionContext context) {
    Tuple intermResult = scriptEngine.getPartialResult();
    PlanProto.Tuple.Builder builder = PlanProto.Tuple.newBuilder();
    for (int i = 0; i < intermResult.size(); i++) {
      builder.addDatums(EvalNodeSerializer.serialize(intermResult.get(i)));
    }
    return new ProtobufDatum(builder.build());
  }

  @Override
  public TajoDataTypes.DataType getPartialResultType() {
    return CatalogUtil.newDataType(TajoDataTypes.Type.PROTOBUF, PlanProto.Tuple.class.getName());
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
