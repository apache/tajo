/**
 * 
 */
package nta.engine.exec.eval;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.annotations.Expose;

import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.engine.json.GsonCreator;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 *
 */
public class BinaryEval extends EvalNode implements Cloneable {
	@Expose private DataType returnType = null;

	/**
	 * @param type
	 */
	public BinaryEval(Type type, EvalNode left, EvalNode right) {
		super(type, left, right);		
		Preconditions.checkNotNull(type);
		Preconditions.checkNotNull(left);
		Preconditions.checkNotNull(right);
		
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
	
	public BinaryEval(PartialBinaryExpr expr) {
	  this(expr.type, expr.leftExpr, expr.rightExpr);
	}

  private DataType determineType(DataType left, DataType right) {
    switch (left) {
      case INT: {
        switch(right) {
          case SHORT:
          case INT: return DataType.INT;
          case LONG: return DataType.LONG;
          case FLOAT:
          case DOUBLE: return DataType.DOUBLE;
          default: throw new InvalidEvalException();
        }
      }

      case LONG: {
        switch(right) {
          case SHORT:
          case INT:
          case LONG: return DataType.LONG;
          case FLOAT:
          case DOUBLE: return DataType.DOUBLE;
          default: throw new InvalidEvalException();
        }
      }

      case FLOAT: {
        switch(right) {
          case SHORT:
          case INT:
          case LONG:
          case FLOAT:
          case DOUBLE: return DataType.DOUBLE;
          default: throw new InvalidEvalException();
        }
      }

      case DOUBLE: {
        switch(right) {
          case SHORT:
          case INT:
          case LONG:
          case FLOAT:
          case DOUBLE: return DataType.DOUBLE;
          default: throw new InvalidEvalException();
        }
      }

      default: return left;
    }
  }

	/* (non-Javadoc)
	 * @see nta.query.executor.eval.Expr#evalBool(nta.storage.Tuple)
	 */
	@Override
	public void eval(Schema schema, Tuple tuple, Datum...args) {
	  leftExpr.eval(schema, tuple);
    rightExpr.eval(schema, tuple);
	}

  @Override
  public Datum terminate() {
    switch(type) {
      case AND:
        return DatumFactory.createBool(leftExpr.terminate().asBool() && rightExpr.terminate().asBool());
      case OR:
        return DatumFactory.createBool(leftExpr.terminate().asBool() || rightExpr.terminate().asBool());

      case EQUAL:
        return leftExpr.terminate().equalsTo(rightExpr.terminate());
      case LTH:
        return leftExpr.terminate().lessThan(rightExpr.terminate());
      case LEQ:
        return leftExpr.terminate().lessThanEqual(rightExpr.terminate());
      case GTH:
        return leftExpr.terminate().greaterThan(rightExpr.terminate());
      case GEQ:
        return leftExpr.terminate().greaterThanEqual(rightExpr.terminate());

      case PLUS:
        return leftExpr.terminate().plus(rightExpr.terminate());
      case MINUS:
        return leftExpr.terminate().minus(rightExpr.terminate());
      case MULTIPLY:
        return leftExpr.terminate().multiply(rightExpr.terminate());
      case DIVIDE:
        return leftExpr.terminate().divide(rightExpr.terminate());
      default:
        throw new InvalidEvalException();
    }
  }

  @Override
	public String getName() {
		return "?";
	}
	
	@Override
	public DataType getValueType() {
		if (returnType == null) {
		  
		}
	  return returnType;
	}
	
	public String toString() {
		return leftExpr +" "+type+" "+rightExpr;
	}
	
	public String toJSON() {
	  Gson gson = GsonCreator.getInstance();
	  return gson.toJson(this, EvalNode.class);
	}
	
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof BinaryEval) {
      BinaryEval other = (BinaryEval) obj;

      boolean b1 = this.type == other.type;
      boolean b2 = leftExpr.equals(other.leftExpr);
      boolean b3 = rightExpr.equals(other.rightExpr);
      return b1 && b2 && b3;      
    }
    return false;
  }
  
  public int hashCode() {
    return Objects.hashCode(this.type, leftExpr, rightExpr);
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    BinaryEval eval = (BinaryEval) super.clone();
    eval.returnType = returnType;
    
    return eval;
  }
}
