/**
 * 
 */
package nta.engine.exec.eval;

import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 *
 */
public class BinaryEval extends EvalNode {
	DataType returnType;

	/**
	 * @param type
	 */
	public BinaryEval(Type type, EvalNode left, EvalNode right) {
		super(type, left, right);
		
		if(
			type == Type.AND ||
			type == Type.OR ||
			type == Type.EQUAL ||
			type == Type.LTH ||
			type == Type.GTH ||
			type == Type.LEQ ||
			type == Type.GEQ
		) {
			this.returnType = DataType.BOOLEAN;
		} else if (
			type == Type.PLUS ||
			type == Type.MINUS ||
			type == Type.MULTIPLY ||
			type == Type.DIVIDE
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
	public Datum eval(Schema schema, Tuple tuple, Datum...args) {
		switch(type) {
		case AND:
			return DatumFactory.createBool(leftExpr.eval(schema, tuple).asBool() && rightExpr.eval(schema, tuple).asBool());
		case OR:
			return DatumFactory.createBool(leftExpr.eval(schema, tuple).asBool() || rightExpr.eval(schema, tuple).asBool());
		
		case EQUAL:
			return leftExpr.eval(schema, tuple).equalsTo(rightExpr.eval(schema, tuple));
		case LTH:
			return leftExpr.eval(schema, tuple).lessThan(rightExpr.eval(schema, tuple));
		case LEQ:
			return leftExpr.eval(schema, tuple).lessThanEqual(rightExpr.eval(schema, tuple));
		case GTH:
			return leftExpr.eval(schema, tuple).greaterThan(rightExpr.eval(schema, tuple));
		case GEQ:
			return leftExpr.eval(schema, tuple).greaterThanEqual(rightExpr.eval(schema, tuple));			
			
		case PLUS:
			return leftExpr.eval(schema, tuple).plus(rightExpr.eval(schema, tuple));			
		case MINUS:
			return leftExpr.eval(schema, tuple).minus(rightExpr.eval(schema, tuple));
		case MULTIPLY:
			return leftExpr.eval(schema, tuple).multiply(rightExpr.eval(schema, tuple));
		case DIVIDE:
			return leftExpr.eval(schema, tuple).divide(rightExpr.eval(schema, tuple));
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
	
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof BinaryEval) {
      BinaryEval other = (BinaryEval) obj;

      if (this.type == other.type && leftExpr.equals(other.leftExpr)
          && rightExpr.equals(other.rightExpr)) {
        return true;
      }
    }
    return false;
  }
}
