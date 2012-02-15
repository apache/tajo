package nta.engine.function;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.engine.exec.eval.EvalNode;
import nta.engine.utils.TUtil;

import com.google.gson.annotations.Expose;

/**
 * @author Hyunsik Choi
 */
public abstract class Function implements Cloneable {
	@Expose
	protected String signature;
	@Expose
  protected Column [] definedArgs;
	@Expose
	protected EvalNode [] givenArgs;
	
	public Function(Column [] definedArgs) {
		this.definedArgs = definedArgs;
	}
	
	public final Column [] getDefinedArgs() {
		return this.definedArgs;
	}
	
	public void setSignature(String signature) {
	  this.signature = signature;
	}
	
	public String getSignature() {
	  return signature;
	}
	
	public abstract Datum invoke(Datum ... datums);
	public abstract DataType getResType();
	
	public enum Type {
	  AGG,
	  GENERAL;
	}
	
	@Override
	public boolean equals(Object obj) {
	  if (obj instanceof Function) {
	    Function other = (Function) obj;
	    return TUtil.checkEquals(signature, other.signature)
	        && TUtil.checkEquals(definedArgs, other.definedArgs)
	        && TUtil.checkEquals(givenArgs, other.givenArgs);	    
	  } else {
	    return false;
	  }
	}
		
	public Object clone() throws CloneNotSupportedException {
	  Function func = (Function) super.clone();
	  func.signature = signature;
	  func.definedArgs = definedArgs != null ? definedArgs.clone() : null;
	  func.givenArgs = givenArgs != null ? givenArgs.clone() : null;
	  
	  return func;
	}
}