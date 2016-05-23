/***
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

package org.apache.tajo.catalog;

import org.apache.tajo.catalog.proto.CatalogProtos.FunctionType;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.function.*;

public class FunctionDescBuilder {
  private String name = null;
  private FunctionType funcType = null;
  private DataType retType = null;
  private DataType [] params = null;
  private Class<? extends Function> clazz = null;
  private UDFInvocationDesc udfInvocation = null;
  private boolean isDeterministic = true;
  private String description = null;
  private String example = null;

  public FunctionDescBuilder setName(String name) {
    this.name = name;
    return this;
  }

  public FunctionDescBuilder setFunctionType(FunctionType type) {
    funcType = type;
    return this;
  }

  public FunctionDescBuilder setReturnType(DataType type) {
    retType = type;
    return this;
  }

  public FunctionDescBuilder setParams(DataType [] params) {
    this.params = params;
    return this;
  }

  public FunctionDescBuilder setClass(Class<? extends Function> clazz) {
    this.clazz = clazz;
    return this;
  }

  public FunctionDescBuilder setDeterministic(boolean isDeterministic) {
    this.isDeterministic = isDeterministic;
    return this;
  }

  public FunctionDescBuilder setDescription(String desc) {
    this.description = desc;
    return this;
  }

  public FunctionDescBuilder setExample(String ex) {
    this.example = ex;
    return this;
  }

  public FunctionDescBuilder setUDF(UDFInvocationDesc udf) {
    this.udfInvocation = udf;
    return this;
  }


  public FunctionDesc build() {
    FunctionInvocation invocation = new FunctionInvocation();
    if (funcType == FunctionType.UDF) {
      invocation.setUDF(udfInvocation);
    }
    else {
      invocation.setLegacy(new ClassBaseInvocationDesc<>(clazz));
    }

    FunctionSupplement supplement = new FunctionSupplement();

    if (description != null) {
      supplement.setShortDescription(description);
    }

    if (example != null) {
      supplement.setExample(example);
    }

    FunctionSignature signature = new FunctionSignature(funcType, name, retType, isDeterministic, params);

    return new FunctionDesc(signature, invocation, supplement);
  }
}
