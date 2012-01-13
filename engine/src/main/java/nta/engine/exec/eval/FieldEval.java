package nta.engine.exec.eval;

import nta.catalog.Column;
import nta.catalog.proto.TableProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.storage.Tuple;

/**
 * 
 * @author Hyunsik Choi
 * 
 */
public class FieldEval extends EvalNode {
	DataType dataType;
	public int tableId;
	public int fieldId;
	public String columnName;
	
	public FieldEval(DataType type, int tableId, int fieldId, String columnName) {
		super(Type.FIELD);
		this.dataType = type;
		this.tableId = tableId;
		this.fieldId = fieldId;
	}
	
	public FieldEval(int tableId, Column col) {
	  super(Type.FIELD);
	  this.dataType = col.getDataType();
	  this.tableId = tableId;
	  this.fieldId = col.getId();
	  this.columnName = col.getName();
	}

	@Override
	public Datum eval(Tuple tuple, Datum...args) {	
		switch(dataType) {
		case INT: return DatumFactory.create(tuple.getInt(fieldId));
		case LONG: return DatumFactory.create(tuple.getLong(fieldId));
		case FLOAT: return DatumFactory.create(tuple.getFloat(fieldId));
		case DOUBLE: return DatumFactory.create(tuple.getDouble(fieldId));
		default: throw new InvalidEvalException();
		}		
	}
	
	@Override
	public DataType getValueType() {
		return dataType;
	}

	@Override
	public String getName() {		
		return columnName;
	}
	
	public String toString() {
		return "FIELD("+fieldId+")";
	}
}
