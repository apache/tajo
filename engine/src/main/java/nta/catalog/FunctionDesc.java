/**
 * 
 */
package nta.catalog;

import java.lang.reflect.Constructor;
import java.util.Arrays;

import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.FunctionDescProto;
import nta.catalog.proto.CatalogProtos.FunctionDescProtoOrBuilder;
import nta.catalog.proto.CatalogProtos.FunctionType;
import nta.common.ProtoObject;
import nta.engine.exception.InternalException;
import nta.engine.function.Function;
import nta.engine.json.GsonCreator;

import com.google.gson.Gson;
import com.google.gson.annotations.Expose;

/**
 * @author Hyunsik Choi
 * 
 */
public class FunctionDesc implements ProtoObject<FunctionDescProto> { 
  private FunctionDescProto proto = FunctionDescProto.getDefaultInstance();
  private FunctionDescProto.Builder builder = null;
  private boolean viaProto = false;
  
  @Expose
  private String signature;
  @Expose
  private Class<? extends Function> funcClass;
  @Expose
  private FunctionType funcType;
  @Expose
  private DataType returnType;
  @Expose
  private DataType [] parameterTypes;

  public FunctionDesc() {
    this.builder = FunctionDescProto.newBuilder();
  }
  
  public FunctionDesc(String signature, Class<? extends Function> clazz,
      FunctionType funcType, DataType retType, DataType... parameterTypes) {
    this();
    this.signature = signature;
    this.funcClass = clazz;
    this.funcType = funcType;
    this.returnType = retType;
    this.parameterTypes = parameterTypes;
  }
  
  public FunctionDesc(FunctionDescProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }

  @SuppressWarnings("unchecked")
  public FunctionDesc(String signature, String className, FunctionType type,
      DataType retType, DataType... argTypes) throws ClassNotFoundException {
    this(signature, (Class<? extends Function>) Class.forName(className), type,
        retType, argTypes);
  }
  
  public static FunctionDesc create(FunctionDescProto proto) {
    return new FunctionDesc(proto);
  }
  
  /**
   * 
   * @return 함수 인스턴스
   * @throws InternalException
   */
  public Function newInstance() throws InternalException {
    try {
      Constructor<? extends Function> cons = getFuncClass().getConstructor();
      Function f = (Function) cons.newInstance();
      return f;
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

  public DataType [] getDefinedArgs() {
    FunctionDescProtoOrBuilder p = viaProto ? proto : builder;
    if (this.parameterTypes != null) {
      return this.parameterTypes;
    }
    if (p.getParameterTypesCount() == 0) {
      return null;
    }
    this.parameterTypes = p.getParameterTypesList().toArray(
        new DataType[p.getParameterTypesCount()]);
    return this.parameterTypes;
  }

  public DataType getReturnType() {
    FunctionDescProtoOrBuilder p = viaProto ? proto : builder;
    if (this.returnType != null) {
      return this.returnType;
    }
    if (!p.hasReturnType()) {
      return null;
    }
    this.returnType = p.getReturnType();
    return this.returnType;
    
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
  
  public Object clone() {
    return new FunctionDesc(getProto());
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
      builder.setReturnType(this.returnType);
    }
    if (this.parameterTypes != null) {
      builder.addAllParameterTypes(Arrays.asList(parameterTypes));
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
			      (Class<? extends Function>)Class.forName(p.getClassName());
		  } catch (ClassNotFoundException e) {
			  throw new InternalException("The function class ("+p.getClassName()+") cannot be loaded");
		  }
	  }
	  if (funcType == null && p.hasType()) {
		  funcType = p.getType();
	  }
	  if (returnType == null && p.hasReturnType()) {
		  returnType = p.getReturnType();
	  }
	  if (parameterTypes == null && p.getParameterTypesCount() > 0) {
		  parameterTypes = new DataType[p.getParameterTypesCount()];
		  for (int i = 0; i < p.getParameterTypesCount(); i++) {
			  parameterTypes[i] = p.getParameterTypes(i);
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
