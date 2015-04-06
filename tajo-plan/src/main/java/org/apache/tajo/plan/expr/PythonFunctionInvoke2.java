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

package org.apache.tajo.plan.expr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;

public class PythonFunctionInvoke2 extends FunctionInvoke {

  private static final Log LOG = LogFactory.getLog(PythonFunctionInvoke2.class);

  private PythonFunctionInvokeContext invokeContext;

  public PythonFunctionInvoke2(FunctionDesc functionDesc) {
    super(functionDesc);
    if (!functionDesc.getInvocation().hasPython()) {
      throw new IllegalStateException("Function type must be python");
    }
  }

  @Override
  public void init(FunctionInvokeContext invokeContext, FunctionEval.ParamType[] paramTypes) throws IOException {
    this.invokeContext = (PythonFunctionInvokeContext) invokeContext;
    this.invokeContext.getExecutor().init(functionDesc);
  }



  @Override
  public Datum eval(Tuple tuple) {
    return invokeContext.getExecutor().getOutput(tuple);
  }
}
