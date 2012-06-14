package nta.engine.exec.eval;

import com.google.gson.Gson;
import com.google.gson.annotations.Expose;

import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.engine.json.GsonCreator;
import nta.storage.Tuple;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public abstract class EvalNode implements Cloneable {
	@Expose
	protected Type type;
	@Expose
	protected EvalNode leftExpr;
	@Expose
	protected EvalNode rightExpr;
	
	public EvalNode(Type type) {
		this.type = type;
	}
	
	public EvalNode(Type type, EvalNode left, EvalNode right) {
		this(type);
		this.leftExpr = left;
		this.rightExpr = right;
	}
	
	public Type getType() {
		return this.type;
	}
	
	public void setLeftExpr(EvalNode expr) {
		this.leftExpr = expr;
	}
	
	public EvalNode getLeftExpr() {
		return this.leftExpr;
	}
	
	public void setRightExpr(EvalNode expr) {
		this.rightExpr = expr;
	}
	
	public EvalNode getRightExpr() {
		return this.rightExpr;
	}

  public EvalNode getExpr(int id) {
    if (id == 0) {
      return this.leftExpr;
    } else if (id == 1) {
      return this.rightExpr;
    } else {
      throw new ArrayIndexOutOfBoundsException("only 0 or 1 is available (" + id + " is not available)");
    }
  }
	
	public abstract DataType getValueType();
	
	public abstract String getName(); 
	
	public String toString() {
		return "("+this.type+"("+leftExpr.toString()+" "+rightExpr.toString()+"))";
	}
	
	public String toJSON() {
	  Gson gson = GsonCreator.getInstance();
    return gson.toJson(this, EvalNode.class);
	}
	
	public void eval(Schema schema, Tuple tuple, Datum...args) {}

  public abstract Datum terminate();

	public void preOrder(EvalNodeVisitor visitor) {
	  visitor.visit(this);
	  leftExpr.preOrder(visitor);
	  rightExpr.preOrder(visitor);
	}
	
	public void postOrder(EvalNodeVisitor visitor) {
	  leftExpr.postOrder(visitor);
	  rightExpr.postOrder(visitor);	  	  
	  visitor.visit(this);
	}
	
	public static enum Type {
    AGG_FUNCTION,
    AND,
	  OR,
	  EQUAL,
	  NOT_EQUAL,
	  LTH,
	  LEQ,
	  GTH,
	  GEQ,
	  NOT,
	  PLUS,
    MINUS,
    MULTIPLY,
    DIVIDE,
	  FIELD,
    FUNCTION,
    LIKE,
    CONST,
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
	  EvalNode node = (EvalNode) super.clone();
	  node.type = type;
	  node.leftExpr = leftExpr != null ? (EvalNode) leftExpr.clone() : null;
	  node.rightExpr = rightExpr != null ? (EvalNode) rightExpr.clone() : null;
	  
	  return node;
	}
}
