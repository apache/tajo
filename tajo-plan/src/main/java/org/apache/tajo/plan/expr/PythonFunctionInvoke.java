///**
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.tajo.plan.expr;
//
//import com.google.common.base.Objects;
//import com.google.gson.annotations.Expose;
//import org.apache.tajo.OverridableConf;
//import org.apache.tajo.catalog.FunctionDesc;
//import org.apache.tajo.catalog.Schema;
//import org.apache.tajo.common.TajoDataTypes;
//import org.apache.tajo.datum.Datum;
//import org.apache.tajo.function.PythonInvocationDesc;
//import org.apache.tajo.plan.function.python.JythonScriptEngine;
//import org.apache.tajo.plan.function.python.JythonUtils;
//import org.apache.tajo.storage.Tuple;
//import org.apache.tajo.util.TUtil;
//import org.python.core.PyFunction;
//import org.python.core.PyObject;
//
//import java.io.IOException;
//import java.util.Arrays;
//
//public class PythonFunctionInvoke extends FunctionInvoke {
//  @Expose private PythonInvocationDesc invokeDesc;
//  @Expose private TajoDataTypes.DataType[] paramTypes;
//
//  public PythonFunctionInvoke(FunctionDesc funcDesc) {
//    super(funcDesc);
//    this.invokeDesc = funcDesc.getInvocation().getPython();
//    this.paramTypes = funcDesc.getSignature().getParamTypes();
//  }
//
//  @Override
//  public void init(OverridableConf queryContext, FunctionEval.ParamType[] paramTypes) {
//    // nothing to do
//  }
//
//  @Override
//  public Datum eval(Tuple tuple) {
//    try {
//      PyFunction function = JythonScriptEngine.getFunction(invokeDesc.getPath(), invokeDesc.getName());
//
//      PyObject result;
//      if (paramTypes.length == 0) {
//        result = function.__call__();
//      } else {
//        // Find the actual data types from the given parameters at runtime,
//        // and convert them into PyObject instances.
//        PyObject[] pyParams = JythonUtils.tupleToPyTuple(tuple).getArray();
//        result = function.__call__(pyParams);
//      }
//
//      return JythonUtils.pyObjectToDatum(result);
//    } catch (IOException e) {
//      throw new RuntimeException(e);
//    }
//  }
//
//  @Override
//  public boolean equals(Object o) {
//    if (o instanceof PythonFunctionInvoke) {
//      PythonFunctionInvoke other = (PythonFunctionInvoke) o;
//      return this.invokeDesc.equals(other.invokeDesc) &&
//          TUtil.checkEquals(this.paramTypes, other.paramTypes);
//    }
//    return false;
//  }
//
//  @Override
//  public int hashCode() {
//    return Objects.hashCode(invokeDesc, Arrays.hashCode(paramTypes));
//  }
//
//  @Override
//  public Object clone() throws CloneNotSupportedException {
//    PythonFunctionInvoke clone = (PythonFunctionInvoke) super.clone();
//    clone.invokeDesc = (PythonInvocationDesc) this.invokeDesc.clone();
//    clone.paramTypes = new TajoDataTypes.DataType[paramTypes.length];
//    paramTypes = Arrays.copyOf(paramTypes, paramTypes.length);
//    return clone;
//  }
//}
