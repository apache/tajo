/*
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

package org.apache.tajo.function;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.proto.CatalogProtos.PythonInvocationDescProto;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.util.TUtil;

/**
 * <code>PythonInvocationDesc</code> describes a function name
 * and a file path to the script where the function is defined.
 */
public class PythonInvocationDesc implements ProtoObject<PythonInvocationDescProto>, Cloneable {
  @Expose private boolean isScalarFunction; // true if udf, false if udaf
  @Expose private String funcOrClassName; // function name if udf, class name if udaf
  @Expose private String filePath; // file path to the python module

  /**
   * Constructor of {@link PythonInvocationDesc}.
   *
   * @param funcOrClassName if udf, function name. else, class name.
   * @param filePath path to script file
   * @param isScalarFunction
   */
  public PythonInvocationDesc(String funcOrClassName, String filePath, boolean isScalarFunction) {
    this.funcOrClassName = funcOrClassName;
    this.filePath = filePath;
    this.isScalarFunction = isScalarFunction;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public PythonInvocationDesc(PythonInvocationDescProto proto) {
    this(proto.getFuncName(), proto.getFilePath(), proto.getIsScalarFunction());
  }

  public String getName() {
    return funcOrClassName;
  }

  public String getPath() {
    return filePath;
  }

  public boolean isScalarFunction() {
    return this.isScalarFunction;
  }

  @Override
  public PythonInvocationDescProto getProto() {
    PythonInvocationDescProto.Builder builder = PythonInvocationDescProto.newBuilder();
    builder.setFuncName(funcOrClassName).setFilePath(filePath).setIsScalarFunction(isScalarFunction);
    return builder.build();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof PythonInvocationDesc) {
      PythonInvocationDesc other = (PythonInvocationDesc) o;
      return TUtil.checkEquals(funcOrClassName, other.funcOrClassName) &&
          TUtil.checkEquals(filePath, other.filePath) && isScalarFunction == other.isScalarFunction;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(funcOrClassName, filePath, isScalarFunction);
  }

  @Override
  public String toString() {
    return isScalarFunction ? "[UDF] " : "[UDAF] " + funcOrClassName + " at " + filePath;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    PythonInvocationDesc clone = (PythonInvocationDesc) super.clone();
    clone.funcOrClassName = funcOrClassName == null ? null : funcOrClassName;
    clone.filePath = filePath == null ? null : filePath;
    clone.isScalarFunction = isScalarFunction;
    return clone;
  }
}
