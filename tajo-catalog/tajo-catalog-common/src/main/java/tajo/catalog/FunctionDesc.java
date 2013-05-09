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

package tajo.catalog;

import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import tajo.catalog.function.Function;
import tajo.catalog.function.GeneralFunction;
import tajo.catalog.json.GsonCreator;
import tajo.catalog.proto.CatalogProtos.FunctionDescProto;
import tajo.catalog.proto.CatalogProtos.FunctionDescProtoOrBuilder;
import tajo.catalog.proto.CatalogProtos.FunctionType;
import tajo.common.ProtoObject;
import tajo.common.TajoDataTypes.DataType;
import tajo.exception.InternalException;

import java.lang.reflect.Constructor;
import java.util.Arrays;

public class FunctionDesc implements ProtoObject<FunctionDescProto>, Cloneable {
  private FunctionDescProto proto = FunctionDescProto.getDefaultInstance();
  private FunctionDescProto.Builder builder = null;
  private boolean viaProto = false;
  
  @Expose private String signature;
  @Expose private Class<? extends Function> funcClass;
  @Expose private FunctionType funcType;
  @Expose private DataType [] returnType;
  @Expose private DataType [] params;

  public FunctionDesc() {
    this.builder = FunctionDescProto.newBuilder();
  }
  
  public FunctionDesc(String signature, Class<? extends Function> clazz,
      FunctionType funcType, DataType [] retType, DataType [] params) {
    this();
    this.signature = signature.toLowerCase();
    this.funcClass = clazz;
    this.funcType = funcType;
    this.returnType = retType;
    this.params = params;
  }
  
  public FunctionDesc(FunctionDescProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }

  @SuppressWarnings("unchecked")
  public FunctionDesc(String signature, String className, FunctionType type,
                      DataType [] retType, DataType... argTypes) throws ClassNotFoundException {
    this(signature, (Class<? extends Function>) Class.forName(className), type,
        retType, argTypes);
  }

  /**
   * 
   * @return 함수 인스턴스
   * @throws InternalException
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
    FunctionDescProtoOrBuilder p = viaProto ? proto : builder;
    if (this.signature != null) {
      return this.signature;
    }
    if (!proto.hasSignature()) {
      return null;
    }
    this.signature = p.getSignature();
    return this.signature;
  }

  @SuppressWarnings("unchecked")
  public Class<? extends Function> getFuncClass() throws InternalException {
    FunctionDescProtoOrBuilder p = viaProto ? proto : builder;
    if (this.funcClass != null) {
      return this.funcClass;
    }
    if (!p.hasClassName()) {
      return null;
    }
    try {
      this.funcClass = (Class<? extends Function>)Class.forName(p.getClassName());
    } catch (ClassNotFoundException e) {
      throw new InternalException("The function class ("+p.getClassName()+") cannot be loaded");
    }
    return this.funcClass;
  }

  public FunctionType getFuncType() {
    FunctionDescProtoOrBuilder p = viaProto ? proto : builder;
    if (this.funcType != null) {
      return this.funcType;
    }
    if (!p.hasType()) {
      return null;
    }
    this.funcType = p.getType();
    return this.funcType;
  }

  public DataType [] getParamTypes() {
    FunctionDescProtoOrBuilder p = viaProto ? proto : builder;
    if (this.params != null) {
      return this.params;
    }
    if (p.getParameterTypesCount() == 0) {
      return null;
    }
    this.params = p.getParameterTypesList().toArray(
        new DataType[p.getParameterTypesCount()]);
    return this.params;
  }

  public DataType [] getReturnType() {
    FunctionDescProtoOrBuilder p = viaProto ? proto : builder;
    if (this.returnType != null) {
      return newNoNameSchema(this.returnType);
    }
    if (!p.hasReturnType()) {
      return null;
    }
    this.returnType = newNoNameSchema(p.getReturnType());
    return this.returnType;
    
  }

  public static DataType [] newNoNameSchema(DataType ... types) {
    DataType [] dataTypes = types.clone();
    return dataTypes;
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
    desc.proto = this.proto;
    desc.builder = this.builder == null?null:this.builder.clone();
    
    desc.signature = this.signature;
    desc.params = this.params;
    
    desc.returnType = this.returnType;
    desc.viaProto = this.viaProto;
    desc.funcClass = this.funcClass;
    
    return desc;
  }

  @Override
  public FunctionDescProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
  
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = FunctionDescProto.newBuilder(proto);
    }
    viaProto = false;
  }
  
  private void mergeLocalToBuilder() {
    if (this.signature  != null) {     
      builder.setSignature(this.signature);
    }
    if (this.funcClass != null) {
      builder.setClassName(this.funcClass.getName());
    }
    if (this.funcType != null) {
      builder.setType(this.funcType);
    }
    if (this.returnType != null) {
      builder.setReturnType(this.returnType[0]);
    }
    if (this.params != null) {
      builder.addAllParameterTypes(Arrays.asList(params));
    }
  }
  
  private void mergeLocalToProto() {
    if(viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }
  
  @SuppressWarnings("unchecked")
  private void mergeProtoToLocal() throws InternalException {
	  FunctionDescProtoOrBuilder p = viaProto ? proto : builder;
	  if (signature == null && p.hasSignature()) {
		  signature = p.getSignature();
	  }
	  if (funcClass == null && p.hasClassName()) {
		  try {
			  this.funcClass = 
			      (Class<? extends GeneralFunction>)Class.forName(p.getClassName());
		  } catch (ClassNotFoundException e) {
			  throw new InternalException("The function class ("+p.getClassName()+") cannot be loaded");
		  }
	  }
	  if (funcType == null && p.hasType()) {
		  funcType = p.getType();
	  }
	  if (returnType == null && p.hasReturnType()) {
		  returnType = newNoNameSchema(p.getReturnType());
	  }
	  if (params == null && p.getParameterTypesCount() > 0) {
		  params = new DataType[p.getParameterTypesCount()];
		  for (int i = 0; i < p.getParameterTypesCount(); i++) {
			  params[i] = p.getParameterTypes(i);
		  }
	  }
  }
  
  @Override
  public String toString() {
	  return getProto().toString();
  }

  @Override
  public void initFromProto() {
    try {
      mergeProtoToLocal();
    } catch (InternalException e) {
      e.printStackTrace();
    }
  }
  
  public String toJSON() {
    initFromProto();
    Gson gson = GsonCreator.getInstance();
    return gson.toJson(this, FunctionDesc.class);
  }
}
