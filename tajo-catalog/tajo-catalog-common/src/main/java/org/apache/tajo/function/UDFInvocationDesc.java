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
import org.apache.tajo.catalog.proto.CatalogProtos.UDFtype;
import org.apache.tajo.catalog.proto.CatalogProtos.UDFinvocationDescProto;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.util.TUtil;

/**
 * <code>UDFInvocationDesc</code> describes a function name
 * and a file path to the script where the function is defined.
 */
public class UDFInvocationDesc implements ProtoObject<UDFinvocationDescProto>, Cloneable {
  @Expose private UDFtype type;
  @Expose private boolean isScalarFunction; // true if udf, false if udaf
  @Expose private String funcOrClassName; // function name if python udf, class name if python udaf or others
  @Expose private String filePath; // file path to the python module, or jar path to hive udf

  public UDFInvocationDesc(UDFtype type, String funcOrClassName, boolean isScalarFunction) {
    this(type, funcOrClassName, null, isScalarFunction);
  }

  /**
   * Constructor of {@link UDFInvocationDesc}.
   *
   * @param funcOrClassName if udf, function name. else, class name.
   * @param filePath path to script file
   * @param isScalarFunction
   */
  public UDFInvocationDesc(UDFtype type, String funcOrClassName, String filePath, boolean isScalarFunction) {
    this.type = type;
    this.funcOrClassName = funcOrClassName;
    this.filePath = filePath;
    this.isScalarFunction = isScalarFunction;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public UDFInvocationDesc(UDFinvocationDescProto proto) {
    this(proto.getType(), proto.getFuncName(), proto.getFilePath(), proto.getIsScalarFunction());
  }

  public UDFtype getType() {
    return type;
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
  public UDFinvocationDescProto getProto() {
    UDFinvocationDescProto.Builder builder = UDFinvocationDescProto.newBuilder();
    builder.setType(type).setFuncName(funcOrClassName).setIsScalarFunction(isScalarFunction);
    if (filePath != null) {
      builder.setFilePath(filePath);
    }
    return builder.build();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof UDFInvocationDesc) {
      UDFInvocationDesc other = (UDFInvocationDesc) o;
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
    return '['+type.toString()+'/'+ (isScalarFunction ? "UDF] " : "UDAF] ") +
        funcOrClassName + (filePath != null ? (" at " + filePath) : "");
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    UDFInvocationDesc clone = (UDFInvocationDesc) super.clone();
    clone.funcOrClassName = funcOrClassName == null ? null : funcOrClassName;
    clone.filePath = filePath == null ? null : filePath;
    clone.isScalarFunction = isScalarFunction;
    return clone;
  }
}
