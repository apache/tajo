/**
 * 
 */
package nta.catalog;

import java.lang.reflect.Constructor;

import nta.catalog.proto.TableProtos.DataType;
import nta.engine.exception.InternalException;
import nta.engine.exec.eval.EvalNode;
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
  private Class<?>[] argTypes;

  /**
	 * 
	 */
  public FunctionDesc(String signature, Class<? extends Function> clazz,
      Function.Type funcType, DataType retType, Class<?>... argTypes) {
    this.signature = signature;
    this.funcClass = clazz;
    this.funcType = funcType;
    this.returnType = retType;
    this.argTypes = argTypes;
  }

  @SuppressWarnings("unchecked")
  public FunctionDesc(String signature, String className, Function.Type type,
      DataType retType, Class<?>... argTypes) throws ClassNotFoundException {
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

  public String getName() {
    return this.signature;
  }

  public Class<? extends Function> getFuncClass() {
    return this.funcClass;
  }

  public Function.Type getType() {
    return this.funcType;
  }

  public Class<?>[] getArgTypes() {
    return this.argTypes;
  }

  public DataType getReturnType() {
    return this.returnType;
  }
}
