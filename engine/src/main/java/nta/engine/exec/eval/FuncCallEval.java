/**
 * 
 */
package nta.engine.exec.eval;

import nta.catalog.FunctionDesc;
import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.engine.function.Function;
import nta.engine.json.GsonCreator;
import nta.engine.utils.TUtil;
import nta.storage.Tuple;

import com.google.gson.Gson;
import com.google.gson.annotations.Expose;

/**
 * @author Hyunsik Choi
 */
public class FuncCallEval extends EvalNode {
	@Expose
	protected FunctionDesc desc;
	@Expose
  protected Function instance;
	@Expose
	protected EvalNode [] givenArgs;

	/**
	 * @param type
	 */
	public FuncCallEval(FunctionDesc desc, Function instance, 
	    EvalNode [] givenArgs) {
		super(Type.FUNCTION);
		this.desc = desc;
		this.instance = instance;
		this.givenArgs = givenArgs;
	}
	
	public EvalNode [] getGivenArgs() {
	  return this.givenArgs;
	}
	
	public DataType getValueType() {
		return this.desc.getReturnType();
	}

	/* (non-Javadoc)
	 * @see nta.query.executor.eval.Expr#evalVal(nta.storage.Tuple)
	 */
	@Override
	public Datum eval(Schema schema, Tuple tuple, Datum...args) {		
		Datum [] data = null;
		
		if(givenArgs != null) {
			data = new Datum[givenArgs.length];

			for(int i=0;i < givenArgs.length; i++) {
				data[i] = givenArgs[i].eval(schema, tuple);
			}
		}

		return instance.invoke(data);
	}

	@Override
	public String getName() {
		return desc.getSignature();
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(int i=0; i < givenArgs.length; i++) {
			sb.append(givenArgs[i]);
			if(i+1 < givenArgs.length)
				sb.append(",");
		}
		return desc.getSignature()+"("+sb+")";
	}
	
	public String toJSON() {
	  Gson gson = GsonCreator.getInstance();
    return gson.toJson(this, EvalNode.class);
	}
	
	@Override
	public boolean equals(Object obj) {
	  if (obj instanceof FuncCallEval) {
      FuncCallEval other = (FuncCallEval) obj;

      boolean b1 = this.type == other.type;
      boolean b2 = TUtil.checkEquals(instance, other.instance);
      boolean b3 = TUtil.checkEquals(desc, other.desc);
      boolean b4 = TUtil.checkEquals(givenArgs, other.givenArgs);
      
      return b1 && b2 && b3 && b4;
	  }
	  
	  return false;
	}
	
	@Override
  public Object clone() throws CloneNotSupportedException {
    FuncCallEval eval = (FuncCallEval) super.clone();
    eval.desc = (FunctionDesc) desc.clone();
    eval.instance = (Function) instance.clone();
    eval.givenArgs = new EvalNode[givenArgs.length];
    for (int i = 0; i < givenArgs.length; i++) {
      eval.givenArgs[i] = (EvalNode) givenArgs[i].clone();
    }    
    return eval;
  }
	
	@Override
	public void accept(EvalNodeVisitor visitor) {
	  for (EvalNode eval : givenArgs) {
	    eval.accept(visitor);
	  }
	  visitor.visit(this);
	}
}