/**
 * 
 */
package nta.engine.executor.eval;

import nta.catalog.proto.TableProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 *
 */
public class BinaryExpr extends Expr {
	DataType returnType;

	/**
	 * @param type
	 */
	public BinaryExpr(ExprType type, Expr left, Expr right) {
		super(type, left, right);
		
		if(
			type == ExprType.AND ||
			type == ExprType.OR ||
			type == ExprType.EQUAL ||
			type == ExprType.LTH ||
			type == ExprType.GTH ||
			type == ExprType.LEQ ||
			type == ExprType.GEQ
		) {
			this.returnType = DataType.BOOLEAN;
		} else if (
			type == ExprType.PLUS ||
			type == ExprType.MINUS ||
			type == ExprType.MULTIPLY ||
			type == ExprType.DIVIDE
		) {
			this.returnType = determineType(left.getValueType(), 
				right.getValueType());
		}
	}
	
	private DataType determineType(DataType left, DataType right) {
		if(left == right) 
			return left;
		else {
			switch (left) {
				case INT: {
					switch(right) {
						case SHORT:
						case LONG:
						case FLOAT:
						case DOUBLE:
						case STRING: return right;
						default: throw new InvalidEvalException(); 
					}					
				}
				
				case LONG: {
					switch(right) {
						case SHORT:
						case INT: return left;
						case FLOAT:
						case DOUBLE:
						case STRING: return right;
						default: throw new InvalidEvalException(); 
					}
				}
				
				case FLOAT: {
					switch(right) {
						case SHORT:
						case INT: return left;
						case LONG: return DataType.DOUBLE;
						case DOUBLE:
						case STRING: return right;
						default: throw new InvalidEvalException(); 
					}
				}
				
				case DOUBLE: {
					switch(right) {
						case SHORT:
						case INT:
						case LONG: return left;
						case STRING: return right;
						default: throw new InvalidEvalException(); 
					}
				}
				
				default: return left;
			}
		}
	}

	/* (non-Javadoc)
	 * @see nta.query.executor.eval.Expr#evalBool(nta.storage.Tuple)
	 */
	@Override
	public Datum eval(Tuple tuple) {
		switch(type) {
		case AND:
			return DatumFactory.create(leftExpr.eval(tuple).asBool() && rightExpr.eval(tuple).asBool());
		case OR:
			return DatumFactory.create(leftExpr.eval(tuple).asBool() || rightExpr.eval(tuple).asBool());
		
		case EQUAL:
			return leftExpr.eval(tuple).equalTo(rightExpr.eval(tuple));
		case LTH:
			return leftExpr.eval(tuple).lessThan(rightExpr.eval(tuple));
		case LEQ:
			return leftExpr.eval(tuple).lessThanEqual(rightExpr.eval(tuple));
		case GTH:
			return leftExpr.eval(tuple).greaterThan(rightExpr.eval(tuple));
		case GEQ:
			return leftExpr.eval(tuple).greaterThanEqual(rightExpr.eval(tuple));			
			
		case PLUS:
			return leftExpr.eval(tuple).plus(rightExpr.eval(tuple));			
		case MINUS:
			return leftExpr.eval(tuple).minus(rightExpr.eval(tuple));
		case MULTIPLY:
			return leftExpr.eval(tuple).multiply(rightExpr.eval(tuple));
		case DIVIDE:
			return leftExpr.eval(tuple).divide(rightExpr.eval(tuple));
		default:
			throw new InvalidEvalException();
		}
	}

	@Override
	public String getName() {
		return "unnamed";
	}
	
	@Override
	public DataType getValueType() {
		return returnType;
	}
	
	public String toString() {
		return leftExpr +" "+type+" "+rightExpr;
	}
}
