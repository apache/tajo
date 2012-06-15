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
	@Expose protected FunctionDesc funcDesc;
	@Expose protected EvalNode [] argEvals;

	public FuncEval(Type type, FunctionDesc funcDesc, EvalNode[] argEvals) {
		super(type);
		this.funcDesc = funcDesc;
		this.argEvals = argEvals;
	}

  @Override
  public EvalContext newContext() {
    FuncCallCtx newCtx = new FuncCallCtx(argEvals);
    return newCtx;
  }
	
	public EvalNode [] getArgs() {
	  return this.argEvals;
	}

  public void setArgs(EvalNode [] args) {
    this.argEvals = args;
  }
	
	public DataType getValueType() {
		return this.funcDesc.getReturnType();
	}

	@Override
	public abstract void eval(EvalContext ctx, Schema schema, Tuple tuple);

  public abstract Datum terminate(EvalContext ctx);

	@Override
	public String getName() {
		return funcDesc.getSignature();
	}

  @Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(int i=0; i < argEvals.length; i++) {
			sb.append(argEvals[i]);
			if(i+1 < argEvals.length)
				sb.append(",");
		}
		return funcDesc.getSignature()+"("+sb+")";
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
      boolean b2 = TUtil.checkEquals(funcDesc, other.funcDesc);
      boolean b3 = TUtil.checkEquals(argEvals, other.argEvals);
      return b1 && b2 && b3;
	  }
	  
	  return false;
	}
	
	@Override
	public int hashCode() {
	  return Objects.hashCode(funcDesc, argEvals);
	}
	
	@Override
  public Object clone() throws CloneNotSupportedException {
    FuncEval eval = (FuncEval) super.clone();
    eval.funcDesc = (FunctionDesc) funcDesc.clone();
    eval.argEvals = new EvalNode[argEvals.length];
    for (int i = 0; i < argEvals.length; i++) {
      eval.argEvals[i] = (EvalNode) argEvals[i].clone();
    }    
    return eval;
  }
	
	@Override
  public void preOrder(EvalNodeVisitor visitor) {
    for (EvalNode eval : argEvals) {
      eval.postOrder(visitor);
    }
    visitor.visit(this);
  }
	
	@Override
	public void postOrder(EvalNodeVisitor visitor) {
	  for (EvalNode eval : argEvals) {
	    eval.postOrder(visitor);
	  }
	  visitor.visit(this);
	}

  protected class FuncCallCtx implements EvalContext {
    EvalContext [] argCtxs;
    FuncCallCtx(EvalNode [] argEvals) {
      argCtxs = new EvalContext[argEvals.length];
      for (int i = 0; i < argEvals.length; i++) {
        argCtxs[i] = argEvals[i].newContext();
      }
    }
  }
}