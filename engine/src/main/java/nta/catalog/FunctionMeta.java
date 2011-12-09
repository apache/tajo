/**
 * 
 */
package nta.catalog;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import nta.engine.executor.eval.Expr;
import nta.engine.function.FuncType;
import nta.engine.function.Function;

/**
 * @author Hyunsik Choi
 *
 */
public class FunctionMeta {	
	String name;
	Class<? extends Function> funcClass;
	FuncType funcType;
	
	/**
	 * 
	 */
	public FunctionMeta(String funcName, Class<? extends Function> clazz, FuncType funcType) {			
		this.name = funcName;
		this.funcClass = clazz;
		this.funcType = funcType;
	}
	
	@SuppressWarnings("unchecked")
	public FunctionMeta(String funcName, String className, FuncType funcType,
			Class<?> [] paratypes) throws ClassNotFoundException {
		this(funcName, (Class<? extends Function>) Class.forName(className), funcType);
	}
	
	@SuppressWarnings("rawtypes")
	public Function newInstance(ColumnBase [] paramInfo, Expr [] params) throws InstantiationException, 
	IllegalAccessException, NoSuchMethodException, SecurityException, 
	IllegalArgumentException, InvocationTargetException {
		Constructor cons = funcClass.getConstructor(new Class [] {Column[].class, Expr[].class});
		Object [] ps = new Object[2];
		ps[0] = paramInfo;
		ps[1] = params;
		
		return (Function) cons.newInstance(ps);
	}
	
	public String getName() {
		return this.name;
	}
	
	public Class<? extends Function> getFunctionClass() {
		return this.funcClass;
	}
	
	public FuncType getType() {
		return this.funcType;
	}
}
