package nta.engine.function;

import nta.catalog.ColumnBase;
import nta.catalog.proto.TableProtos.DataType;
import nta.datum.Datum;
import nta.engine.exec.eval.EvalNode;


public abstract class Function {
	protected String signature;
  protected ColumnBase [] definedArgs;
	protected EvalNode [] givenArgs;
	
	public Function(ColumnBase [] definedArgs) {		
		this.definedArgs = definedArgs;
	}
	
	public final ColumnBase [] getDefinedArgs() {
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
}
