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
import org.apache.tajo.datum.Datum;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.TUtil;

/**
 * This class invokes class-based scala functions.
 */
public class ClassBasedScalarFunctionInvoke extends FunctionInvoke implements Cloneable {
  @Expose private GeneralFunction function;

  public ClassBasedScalarFunctionInvoke() {

  }

  public ClassBasedScalarFunctionInvoke(FunctionDesc funcDesc) throws InternalException {
    super(funcDesc);
    function = (GeneralFunction) funcDesc.newInstance();
  }

  @Override
  public void setFunctionDesc(FunctionDesc desc) throws InternalException {
    super.setFunctionDesc(desc);
    function = (GeneralFunction) functionDesc.newInstance();
  }

  @Override
  public void init(FunctionInvokeContext context) {
    function.init(context.getQueryContext(), context.getParamTypes());
  }

  @Override
  public Datum eval(Tuple tuple) {
    return function.eval(tuple);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ClassBasedScalarFunctionInvoke) {
      ClassBasedScalarFunctionInvoke other = (ClassBasedScalarFunctionInvoke) o;
      return super.equals(other) &&
          TUtil.checkEquals(function, other.function);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return function.hashCode();
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    ClassBasedScalarFunctionInvoke clone = (ClassBasedScalarFunctionInvoke) super.clone();
    clone.function = (GeneralFunction) function.clone();
    return clone;
  }
}
