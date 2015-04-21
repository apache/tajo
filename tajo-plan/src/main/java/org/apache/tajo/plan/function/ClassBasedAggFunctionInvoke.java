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

import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;

/**
 * This class invokes class-based aggregation functions.
 */
public class ClassBasedAggFunctionInvoke extends AggFunctionInvoke implements Cloneable {
  @Expose private AggFunction function;

  public ClassBasedAggFunctionInvoke(FunctionDesc functionDesc) throws InternalException {
    super(functionDesc);
    function = (AggFunction) functionDesc.newInstance();
  }

  @Override
  public void init(FunctionInvokeContext context) throws IOException {
    // nothing to do
  }

  @Override
  public FunctionContext newContext() {
    return function.newContext();
  }

  @Override
  public void eval(FunctionContext context, Tuple params) {
    function.eval(context, params);
  }

  @Override
  public void merge(FunctionContext context, Tuple params) {
    function.merge(context, params);
  }

  @Override
  public Datum getPartialResult(FunctionContext context) {
    return function.getPartialResult(context);
  }

  @Override
  public TajoDataTypes.DataType getPartialResultType() {
    return function.getPartialResultType();
  }

  @Override
  public Datum terminate(FunctionContext context) {
    return function.terminate(context);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    ClassBasedAggFunctionInvoke clone = (ClassBasedAggFunctionInvoke) super.clone();
    clone.function = (AggFunction) function.clone();
    return clone;
  }
}
