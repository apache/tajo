/**
 * 
 */
package nta.engine.exec.eval;

import com.google.gson.Gson;
import com.google.gson.annotations.Expose;

import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumType;
import nta.engine.json.GsonCreator;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 *
 */
public class ConstEval extends EvalNode implements Comparable<ConstEval>, Cloneable {
	@Expose
	Datum datum = null;
	/**
	 * @param type
	 */
	public ConstEval() {
		super(Type.CONST);
	}
	
	public ConstEval(Datum datum) {
		super(Type.CONST);
		this.datum = datum;
	}
	
	public DatumType getDatumType() {
		return this.datum.type();
	}	

	/* (non-Javadoc)
	 * @see nta.query.executor.expr.Expr#evalVal(nta.storage.Tuple)
	 */
	@Override
	public Datum eval(Schema schema, Tuple tuple, Datum...args) {
		return this.datum;
	}
	
	public String toString() {
		return datum.toString();
	}
	
	public String toJSON() {
		Gson gson = GsonCreator.getInstance();
		return gson.toJson(this, EvalNode.class);
	}
	

	@Override
	public DataType getValueType() {
		switch(this.datum.type()) {
		case BOOLEAN: return DataType.BOOLEAN;
		case BYTE: return DataType.BYTE;
		case BYTES : return DataType.BYTES;
		case DOUBLE : return DataType.DOUBLE;
		case FLOAT: return DataType.FLOAT;
		case INT: return DataType.INT;
		case IPv4: return DataType.IPv4;
		case LONG: return DataType.LONG;
		case SHORT: return DataType.SHORT;
		case STRING: return DataType.STRING;
		default: return DataType.ANY;
		}
	}

	@Override
	public String getName() {
		return this.datum.toString();
	}
	
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ConstEval) {
      ConstEval other = (ConstEval) obj;

      if (this.type == other.type && this.datum.equals(other.datum)) {
        return true;
      }
    }
    return false;
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    ConstEval eval = (ConstEval) super.clone();
    eval.datum = datum;
    
    return eval;
  }

  @Override
  public int compareTo(ConstEval other) {    
    return datum.compareTo(other.datum);
  }
  
  @Override
  public void accept(EvalNodeVisitor visitor) {
    visitor.visit(this);
  }
}
