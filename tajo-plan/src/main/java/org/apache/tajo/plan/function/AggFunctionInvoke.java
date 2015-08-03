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
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;

public abstract class AggFunctionInvoke implements Cloneable {
  @Expose protected FunctionDesc functionDesc;

  public AggFunctionInvoke(FunctionDesc functionDesc) {
    this.functionDesc = functionDesc;
  }

  public static AggFunctionInvoke newInstance(FunctionDesc desc) throws InternalException {
    // TODO: The below line is due to the bug in the function type. The type of class-based functions is not set properly.
    if (desc.getInvocation().hasLegacy()) {
      return new ClassBasedAggFunctionInvoke(desc);
    } else if (desc.getInvocation().hasPythonAggregation()) {
      return new PythonAggFunctionInvoke(desc);
    } else {
      throw new UnsupportedException(desc.getInvocation() + " is not supported");
    }
  }

  public void setFunctionDesc(FunctionDesc functionDesc) {
    this.functionDesc = functionDesc;
  }

  public abstract void init(FunctionInvokeContext context) throws IOException;

  public abstract FunctionContext newContext();

  public abstract void eval(FunctionContext context, Tuple params);

  public abstract void merge(FunctionContext context, Tuple params);

  public abstract Datum getPartialResult(FunctionContext context);

  // TODO: use {@link IntermFunctionSignature} instead of this function.
  public abstract TajoDataTypes.DataType getPartialResultType();

  public abstract Datum terminate(FunctionContext context);

  @Override
  public boolean equals(Object o) {
    if (o instanceof AggFunctionInvoke) {
      AggFunctionInvoke other = (AggFunctionInvoke) o;
      return this.functionDesc.equals(other.functionDesc);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return functionDesc.hashCode();
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    AggFunctionInvoke clone = (AggFunctionInvoke) super.clone();
    clone.functionDesc = (FunctionDesc) this.functionDesc.clone();
    return clone;
  }
}
