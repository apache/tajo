package nta.engine.executor.eval;

import nta.catalog.proto.TableProtos.DataType;
import nta.datum.Datum;
import nta.storage.Tuple;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public abstract class Expr {
	protected ExprType type;
	protected Expr leftExpr;
	protected Expr rightExpr;
	
	public Expr(ExprType type) {
		this.type = type;
	}
	
	public Expr(ExprType type, Expr left, Expr right) {
		this(type);
		this.leftExpr = left;
		this.rightExpr = right;
	}
	
	public ExprType getType() {
		return this.type;
	}
	
	public void setLeftExpr(Expr expr) {
		this.leftExpr = expr;
	}
	
	public Expr getLeftExpr() {
		return this.leftExpr;
	}
	
	public void setRightExpr(Expr expr) {
		this.rightExpr = expr;
	}
	
	public Expr getRightExpr() {
		return this.rightExpr;
	}
	
	public abstract DataType getValueType();
	
	public abstract String getName(); 
	
	public String toString() {
		return "("+this.type+"("+leftExpr.toString()+" "+rightExpr.toString()+"))";
	}
	
	public abstract Datum eval(Tuple tuple);
}
