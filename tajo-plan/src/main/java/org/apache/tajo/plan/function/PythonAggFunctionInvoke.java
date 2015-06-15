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
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.plan.function.python.PythonScriptEngine;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;

public class PythonAggFunctionInvoke extends AggFunctionInvoke implements Cloneable {

  private transient PythonScriptEngine scriptEngine;
  private transient PythonAggFunctionContext prevContext;
  private static int nextContextId = 0;

  /**
   * Aggregated result should be kept in Tajo task rather than Python UDAF to control memory usage.
   * {@link PythonAggFunctionContext} is to support executing aggregation with keys.
   * It stores a snapshot of Python UDAF class instance as a json string.
   *
   * For each UDAF call with different aggregation key,
   * {@link PythonAggFunctionInvoke} calls {@link PythonAggFunctionInvoke#updateContextIfNecessary} to backup and restore
   * intermediate aggregation states for the previous key and the current key, respectively.
   */
  public static class PythonAggFunctionContext implements FunctionContext {
    final int id; // id to identify each context
    String jsonData; // snapshot of Python class

    public PythonAggFunctionContext() {
      this.id = nextContextId++;
    }

    public void setJsonData(String jsonData) {
      this.jsonData = jsonData;
    }

    public String getJsonData() {
      return jsonData;
    }
  }

  public PythonAggFunctionInvoke(FunctionDesc functionDesc) {
    super(functionDesc);
  }

  @Override
  public void init(FunctionInvokeContext context) throws IOException {
    this.scriptEngine = (PythonScriptEngine) context.getScriptEngine();
  }

  @Override
  public FunctionContext newContext() {
    return new PythonAggFunctionContext();
  }

  /**
   * Context does not need to be updated per every UDAF call.
   * If the current aggregation key is same with the previous one,
   * python-side context doesn't need to be updated because it already contains necessary intermediate result.
   *
   * @param context
   */
  private void updateContextIfNecessary(FunctionContext context) {
    PythonAggFunctionContext givenContext = (PythonAggFunctionContext) context;
    if (prevContext == null || prevContext.id != givenContext.id) {
      try {
        if (prevContext != null) {
          scriptEngine.updateJavaSideContext(prevContext);
        }
        scriptEngine.updatePythonSideContext(givenContext);
        prevContext = givenContext;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void eval(FunctionContext context, Tuple params) {
    updateContextIfNecessary(context);
    scriptEngine.callAggFunc(context, params);
  }

  @Override
  public void merge(FunctionContext context, Tuple params) {
    if (params.isBlankOrNull(0)) {
      return;
    }

    updateContextIfNecessary(context);
    scriptEngine.callAggFunc(context, params);
  }

  @Override
  public Datum getPartialResult(FunctionContext context) {
    updateContextIfNecessary(context);
    // partial results are stored as json strings.
    return DatumFactory.createText(scriptEngine.getPartialResult(context));
  }

  @Override
  public TajoDataTypes.DataType getPartialResultType() {
    return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.TEXT);
  }

  @Override
  public Datum terminate(FunctionContext context) {
    updateContextIfNecessary(context);
    return scriptEngine.getFinalResult(context);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    // nothing to do
    return super.clone();
  }
}
