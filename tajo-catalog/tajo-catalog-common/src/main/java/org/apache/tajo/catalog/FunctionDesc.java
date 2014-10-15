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

package org.apache.tajo.catalog;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import org.apache.tajo.annotation.NotNull;
import org.apache.tajo.function.*;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos.FunctionDescProto;
import org.apache.tajo.catalog.proto.CatalogProtos.FunctionType;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.exception.InternalException;

import java.lang.reflect.Constructor;

/**
 * FunctionDesc specifies the description of a function used in Tajo. It consists of three parts:
 * function definition, invocation description (how to invoke this function), and supplement.
 *
 */
public class FunctionDesc implements ProtoObject<FunctionDescProto>, Cloneable, GsonObject, Comparable<FunctionDesc> {
  private FunctionDescProto.Builder builder = FunctionDescProto.newBuilder();

  @Expose private FunctionSignature signature;
  @Expose private FunctionInvocation invocation;
  @Expose private FunctionSupplement supplement;

  public FunctionDesc() {
  }

  public FunctionDesc(String signature, Class<? extends Function> clazz,
      FunctionType funcType, DataType retType, @NotNull DataType [] params) {
    this.signature = new FunctionSignature(funcType, signature.toLowerCase(), retType, params);
    this.invocation = new FunctionInvocation();
    this.invocation.setLegacy(new ClassBaseInvocationDesc<Function>(clazz));
    this.supplement = new FunctionSupplement();
  }

  public FunctionDesc(FunctionDescProto proto) throws ClassNotFoundException {
    this.signature = new FunctionSignature(proto.getSignature());
    this.invocation = new FunctionInvocation(proto.getInvocation());
    this.supplement = new FunctionSupplement(proto.getSupplement());
  }

  public FunctionDesc(String signature, String className, FunctionType type,
                      DataType retType,
                      @NotNull DataType... argTypes) throws ClassNotFoundException {
    this(signature, (Class<? extends Function>) Class.forName(className), type, retType, argTypes);
  }

  public FunctionDesc(FunctionSignature signature, FunctionInvocation invocation, FunctionSupplement supplement) {
    this.signature = signature;
    this.invocation = invocation;
    this.supplement = supplement;
  }

  public FunctionSignature getSignature() {
    return signature;
  }

  public FunctionInvocation getInvocation() {
    return invocation;
  }

  public FunctionSupplement getSupplement() {
    return supplement;
  }

  /**
   * 
   * @return Function Instance
   * @throws org.apache.tajo.exception.InternalException
   */
  public Function newInstance() throws InternalException {
    try {
      Constructor<? extends Function> cons = getFuncClass().getConstructor();
      return cons.newInstance();
    } catch (Exception ioe) {
      throw new InternalException("Cannot initiate function " + signature);
    }
  }

  ////////////////////////////////////////
  // Function Signature
  ////////////////////////////////////////

  public FunctionType getFuncType() {
    return signature.getFunctionType();
  }

  public String getFunctionName() {
    return signature.getName();
  }

  public DataType [] getParamTypes() {
    return signature.getParamTypes();
  }

  public DataType getReturnType() {
    return signature.getReturnType();
  }

  ////////////////////////////////////////
  // Invocation
  ////////////////////////////////////////

  @SuppressWarnings("unchecked")
  public Class<? extends Function> getFuncClass() {
    return invocation.getLegacy().getFunctionClass();
  }

  ////////////////////////////////////////
  // Supplement
  ////////////////////////////////////////

  public String getDescription() {
    return supplement.getShortDescription();
  }

  public String getDetail() {
    return supplement.getDetail();
  }

  public String getExample() {
    return supplement.getExample();
  }

  public void setDescription(String description) {
    supplement.setShortDescription(description);
  }

  public void setDetail(String detail) {
    supplement.setDetail(detail);
  }

  public void setExample(String example) {
    supplement.setExample(example);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(signature);
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FunctionDesc) {
      FunctionDesc other = (FunctionDesc) obj;
      if(this.getProto().equals(other.getProto()))
        return true;
    }
    return false;
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException{
    FunctionDesc desc  = (FunctionDesc)super.clone();
    
    desc.signature = signature.clone();
    desc.supplement = supplement.clone();
    desc.invocation = this.invocation;

    return desc;
  }

  @Override
  public FunctionDescProto getProto() {
    if (builder == null) {
      builder = FunctionDescProto.newBuilder();
    } else {
      builder.clear();
    }
    builder.setSignature(signature.getProto());
    builder.setSupplement(supplement.getProto());
    builder.setInvocation(invocation.getProto());
    return builder.build();
  }
  
  @Override
  public String toString() {
	  return getHelpSignature();
  }
  
  public String toJson() {
    return CatalogGsonHelper.toJson(this, FunctionDesc.class);
  }

  public String getHelpSignature() {
    return signature.toString();
  }

  @Override
  public int compareTo(FunctionDesc o) {
    return signature.compareTo(o.getSignature());
  }
}
