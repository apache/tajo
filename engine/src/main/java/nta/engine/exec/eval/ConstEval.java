/**
 * 
 */
package nta.engine.exec.eval;

import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumType;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 *
 */
public class ConstEval extends EvalNode implements Comparable<ConstEval> {
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
  public int compareTo(ConstEval other) {    
    return datum.compareTo(other.datum);
  }
}
