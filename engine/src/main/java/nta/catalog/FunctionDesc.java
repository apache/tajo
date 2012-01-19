/**
 * 
 */
package nta.catalog;

import java.lang.reflect.Constructor;
import java.util.Arrays;

import nta.catalog.proto.TableProtos.DataType;
import nta.engine.exception.InternalException;
import nta.engine.function.Function;

/**
 * @author Hyunsik Choi
 * 
 */
public class FunctionDesc {
  private String signature;
  private Class<? extends Function> funcClass;
  private Function.Type funcType;
  private DataType returnType;
  private DataType [] argTypes;

  /**
	 * 
	 */
  public FunctionDesc(String signature, Class<? extends Function> clazz,
      Function.Type funcType, DataType retType, DataType... argTypes) {
    this.signature = signature;
    this.funcClass = clazz;
    this.funcType = funcType;
    this.returnType = retType;
    this.argTypes = argTypes;
  }

  @SuppressWarnings("unchecked")
  public FunctionDesc(String signature, String className, Function.Type type,
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
    return this.signature;
  }

  public Class<? extends Function> getFuncClass() {
    return this.funcClass;
  }

  public Function.Type getFuncType() {
    return this.funcType;
  }

  public DataType [] getDefinedArgs() {
    return this.argTypes;
  }

  public DataType getReturnType() {
    return this.returnType;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FunctionDesc) {
      FunctionDesc other = (FunctionDesc) obj;
      if (this.funcClass.getClass().equals(other.funcClass) &&
          this.signature.equals(other.signature) && 
          Arrays.equals(this.argTypes, other.argTypes) &&
          this.returnType.equals(other.returnType) &&
          this.funcType.equals(other.funcType)) {
        return true;
      }
    }
    return false;
  }
}
