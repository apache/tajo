/*
 * Lisensed to the Apache Software Foundation (ASF) under one
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

import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.plan.function.python.PythonScriptEngine;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;

/**
 * This class invokes the python functions.
 */
public class PythonFunctionInvoke extends FunctionInvoke implements Cloneable {

  private transient PythonScriptEngine scriptEngine;

  public PythonFunctionInvoke() {

  }

  public PythonFunctionInvoke(FunctionDesc functionDesc) {
    super(functionDesc);
  }

  @Override
  public void init(FunctionInvokeContext context) throws IOException {
    this.scriptEngine = (PythonScriptEngine) context.getScriptEngine();
  }

  @Override
  public Datum eval(Tuple tuple) {
    Datum res = scriptEngine.callScalarFunc(tuple);
    return res;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    // nothing to do
    return super.clone();
  }
}
