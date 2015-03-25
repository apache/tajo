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

package org.apache.tajo.plan.expr;

import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.function.PythonInvocationDesc;
import org.apache.tajo.plan.function.python.JythonScriptEngine;
import org.apache.tajo.plan.function.python.JythonUtils;
import org.apache.tajo.storage.Tuple;
import org.python.core.PyFunction;
import org.python.core.PyObject;

import java.io.IOException;

public class GeneralPythonFunctionEval extends FunctionEval {

  public GeneralPythonFunctionEval(FunctionDesc funcDesc, EvalNode[] argEvals) {
    super(EvalType.FUNCTION, funcDesc, argEvals);
  }

  @Override
  public Datum eval(Schema schema, Tuple tuple) {
    PythonInvocationDesc desc = funcDesc.getInvocation().getPython();
    try {
      PyFunction function = JythonScriptEngine.getFunction(desc.getPath(), desc.getName());
      TajoDataTypes.DataType[] paramTypes = funcDesc.getSignature().getParamTypes();
      PyObject result;
      if (tuple.size() == 0 || paramTypes.length == 0) {
        result = function.__call__();
      } else {
        PyObject[] params = JythonUtils.tupleToPyTuple(tuple).getArray();
        result = function.__call__(params);
      }
      // TODO: result to datum
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }
}
