/**
 * 
 */
package nta.engine.exec.eval;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import nta.catalog.FunctionDesc;
import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.engine.json.GsonCreator;
import nta.engine.utils.TUtil;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public abstract class FuncEval extends EvalNode {
	@Expose protected FunctionDesc desc;
	@Expose protected EvalNode [] givenArgs;

	public FuncEval(Type type, FunctionDesc desc, EvalNode[] givenArgs) {
		super(type);
		this.desc = desc;
		this.givenArgs = givenArgs;
	}
	
	public EvalNode [] getArgs() {
	  return this.givenArgs;
	}

  public void setArgs(EvalNode [] args) {
    this.givenArgs = args;
  }
	
	public DataType getValueType() {
		return this.desc.getReturnType();
	}

  public abstract void init();

	@Override
	public abstract void eval(Schema schema, Tuple tuple, Datum...args);

  public abstract Datum terminate();

	@Override
	public String getName() {
		return desc.getSignature();
	}

  @Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(int i=0; i < givenArgs.length; i++) {
			sb.append(givenArgs[i]);
			if(i+1 < givenArgs.length)
				sb.append(",");
		}
		return desc.getSignature()+"("+sb+")";
	}

  @Override
	public String toJSON() {
	  Gson gson = GsonCreator.getInstance();
    return gson.toJson(this, EvalNode.class);
	}
	
	@Override
	public boolean equals(Object obj) {
	  if (obj instanceof FuncEval) {
      FuncEval other = (FuncEval) obj;

      boolean b1 = this.type == other.type;
      boolean b2 = TUtil.checkEquals(desc, other.desc);
      boolean b3 = TUtil.checkEquals(givenArgs, other.givenArgs);
      return b1 && b2 && b3;
	  }
	  
	  return false;
	}
	
	@Override
	public int hashCode() {
	  return Objects.hashCode(desc, givenArgs);
	}
	
	@Override
  public Object clone() throws CloneNotSupportedException {
    FuncEval eval = (FuncEval) super.clone();
    eval.desc = (FunctionDesc) desc.clone();
    eval.givenArgs = new EvalNode[givenArgs.length];
    for (int i = 0; i < givenArgs.length; i++) {
      eval.givenArgs[i] = (EvalNode) givenArgs[i].clone();
    }    
    return eval;
  }
	
	@Override
  public void preOrder(EvalNodeVisitor visitor) {
    for (EvalNode eval : givenArgs) {
      eval.postOrder(visitor);
    }
    visitor.visit(this);
  }
	
	@Override
	public void postOrder(EvalNodeVisitor visitor) {
	  for (EvalNode eval : givenArgs) {
	    eval.postOrder(visitor);
	  }
	  visitor.visit(this);
	}
}