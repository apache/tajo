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

package org.apache.tajo.function;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import org.apache.tajo.annotation.NotNull;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.util.TUtil;

import static org.apache.tajo.catalog.proto.CatalogProtos.FunctionSignatureProto;
import static org.apache.tajo.catalog.proto.CatalogProtos.FunctionType;
import static org.apache.tajo.common.TajoDataTypes.DataType;

public class FunctionSignature implements Comparable<FunctionSignature>, ProtoObject<FunctionSignatureProto>,
    Cloneable {

  @Expose
  private FunctionType functionType;
  @Expose
  private String name;
  @Expose
  private DataType[] paramTypes;
  @Expose
  private DataType returnType;

  public FunctionSignature(FunctionType type, String name, DataType returnType, @NotNull DataType... params) {
    this.functionType = type;
    this.name = name;
    this.returnType = returnType;
    this.paramTypes = params;
  }

  public FunctionSignature(FunctionSignatureProto proto) {
    this.functionType = proto.getType();
    this.name = proto.getName();
    this.paramTypes = proto.getParameterTypesList().toArray(new DataType[proto.getParameterTypesCount()]);
    this.returnType = proto.getReturnType();
  }

  public FunctionType getFunctionType() {
    return functionType;
  }

  public String getName() {
    return name;
  }

  public DataType[] getParamTypes() {
    return paramTypes;
  }

  public DataType getReturnType() {
    return returnType;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(functionType, name, returnType, Objects.hashCode(paramTypes));
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FunctionSignature) {
      FunctionSignature other = (FunctionSignature) obj;

      boolean eq = functionType.equals(other.functionType);
      eq = eq && name.equals(other.name);
      eq = eq && TUtil.checkEquals(paramTypes, other.paramTypes);
      eq = eq && returnType.equals(other.returnType);
      return eq;
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return FunctionUtil.buildFQFunctionSignature(name, returnType, paramTypes);
  }

  public FunctionSignature clone() throws CloneNotSupportedException {
    FunctionSignature newSignature = (FunctionSignature) super.clone();
    newSignature.functionType = functionType;
    newSignature.name = name;
    newSignature.returnType = returnType;
    newSignature.paramTypes = paramTypes;
    return newSignature;
  }

  @Override
  public FunctionSignatureProto getProto() {
    FunctionSignatureProto.Builder builder = FunctionSignatureProto.newBuilder();
    builder.setType(functionType);
    builder.setName(name);
    builder.addAllParameterTypes(TUtil.newList(paramTypes));
    builder.setReturnType(returnType);
    return builder.build();
  }

  @Override
  public int compareTo(FunctionSignature o) {
    int cmpVal = name.compareTo(o.name);

    if (cmpVal != 0) {
      return cmpVal;
    }

    cmpVal = returnType.getType().compareTo(o.returnType.getType());

    if (cmpVal != 0) {
      return cmpVal;
    }

    for (int i = 0; i < Math.min(paramTypes.length, o.paramTypes.length); i++) {
      cmpVal = paramTypes[i].getType().compareTo(o.paramTypes[i].getType());

      if (cmpVal != 0) {
        return cmpVal;
      }
    }

    return o.paramTypes.length - paramTypes.length;
  }
}
