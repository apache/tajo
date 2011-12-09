package nta.engine.executor.eval;

import nta.catalog.proto.TableProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.storage.Tuple;

/**
 * 
 * @author Hyunsik Choi
 * 
 */
public class FieldExpr extends Expr {
	DataType type;
	public int tableId;
	public int fieldId;
	
	public FieldExpr(DataType type, int tableId, int fieldId) {
		super(ExprType.FIELD);
		this.type = type;
		this.tableId = tableId;
		this.fieldId = fieldId;
	}

	@Override
	public Datum eval(Tuple tuple) {	
		switch(type) {
		case INT: return DatumFactory.create(tuple.getInt(fieldId));
		case LONG: return DatumFactory.create(tuple.getLong(fieldId));
		case FLOAT: return DatumFactory.create(tuple.getFloat(fieldId));
		case DOUBLE: return DatumFactory.create(tuple.getDouble(fieldId));
		default: throw new InvalidEvalException();
		}		
	}
	
	@Override
	public DataType getValueType() {
		return type;
	}

	@Override
	public String getName() {		
		return "fieldName";
	}
	
	public String toString() {
		return "FIELD("+tableId+","+fieldId+")";
	}
}
