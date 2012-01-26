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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Hyunsik Choi
 * 
 */
public class FunctionDesc implements ProtoObject<FunctionDescProto> {
  private final Log LOG = LogFactory.getLog(FunctionDesc.class);
  
  private FunctionDescProto proto = FunctionDescProto.getDefaultInstance();
  private FunctionDescProto.Builder builder = null;
  private boolean viaProto = false;
  
  private String signature;
  private Class<? extends Function> funcClass;
  private FunctionType funcType;
  private DataType returnType;
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
  
  /**
   * 
   * @return 함수 인스턴스
   * @throws InternalException
   */
  public Function newInstance() throws InternalException {
    try {
      Constructor<? extends Function> cons = funcClass.getConstructor();
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
      /*if (this.funcClass.getClass().equals(other.funcClass) &&
          this.signature.equals(other.signature) && 
          Arrays.equals(this.parameterTypes, other.parameterTypes) &&
          this.returnType.equals(other.returnType) &&
          this.funcType.equals(other.funcType)) {
        return true;
      }*/
      if(this.getProto().equals(other.getProto()));
    }
    return false;
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
}
