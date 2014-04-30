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
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.catalog.function.Function;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos.FunctionDescProto;
import org.apache.tajo.catalog.proto.CatalogProtos.FunctionType;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.exception.InternalException;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;

public class FunctionDesc implements ProtoObject<FunctionDescProto>, Cloneable, GsonObject {
  private FunctionDescProto.Builder builder = FunctionDescProto.newBuilder();
  
  @Expose private String signature;
  @Expose private Class<? extends Function> funcClass;
  @Expose private FunctionType funcType;
  @Expose private DataType returnType;
  @Expose private DataType [] params;
  @Expose private String description;
  @Expose private String detail;
  @Expose private String example;

  public FunctionDesc() {
  }

  public FunctionDesc(String signature, Class<? extends Function> clazz,
      FunctionType funcType, DataType retType,
      DataType [] params) {
    this.signature = signature.toLowerCase();
    this.funcClass = clazz;
    this.funcType = funcType;
    this.returnType = retType;
    this.params = params;
  }

  public FunctionDesc(FunctionDescProto proto) throws ClassNotFoundException {
    this(proto.getSignature(), proto.getClassName(), proto.getType(),
        proto.getReturnType(),
        proto.getParameterTypesList().toArray(new DataType[proto.getParameterTypesCount()]));
    if (proto.hasDescription()) {
      this.description = proto.getDescription();
    }
    if (proto.hasDetail()) {
      this.detail = proto.getDetail();
    }
    if (proto.hasExample()) {
      this.example = proto.getExample();
    }
  }

  public FunctionDesc(String signature, String className, FunctionType type,
                      DataType retType,
                      DataType... argTypes) throws ClassNotFoundException {
    this(signature, (Class<? extends Function>) Class.forName(className), type, retType, argTypes);
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

  public String getSignature() {
    return this.signature;
  }

  @SuppressWarnings("unchecked")
  public Class<? extends Function> getFuncClass() throws InternalException {
    return this.funcClass;
  }

  public FunctionType getFuncType() {
    return this.funcType;
  }

  public DataType [] getParamTypes() {
    return this.params;
  }

  public DataType getReturnType() {
    return this.returnType;
  }

  public String getDescription() {
    return description;
  }

  public String getDetail() {
    return detail;
  }

  public String getExample() {
    return example;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public void setDetail(String detail) {
    this.detail = detail;
  }

  public void setExample(String example) {
    this.example = example;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(signature, Objects.hashCode(params));
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
    
    desc.signature = this.signature;
    desc.params = params.clone();
    desc.description = this.description;
    desc.example = this.example;
    desc.detail = this.detail;
    desc.returnType = this.returnType;
    desc.funcClass = this.funcClass;
    
    return desc;
  }

  @Override
  public FunctionDescProto getProto() {
    if (builder == null) {
      builder = FunctionDescProto.newBuilder();
    } else {
      builder.clear();
    }
    builder.setSignature(this.signature);
    builder.setClassName(this.funcClass.getName());
    builder.setType(this.funcType);
    builder.setReturnType(this.returnType);
    if(this.description != null) {
      builder.setDescription(this.description);
    }
    if (this.detail != null) {
      builder.setDetail(this.detail);
    }
    if (this.example != null) {
      builder.setExample(this.example);
    }
    if (this.params != null) { // repeated field
      builder.addAllParameterTypes(Arrays.asList(params));
    }
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
    return returnType.getType() + " " + CatalogUtil.getCanonicalSignature(signature, getParamTypes());
  }

  public static String dataTypesToStr(List<DataType> parameterTypesList) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < parameterTypesList.size(); i++) {
      DataType eachType = parameterTypesList.get(i);

      if (i > 0) {
        result.append(",");
      }
      result.append(eachType.getType().toString());

    }

    return result.toString();
  }
}
