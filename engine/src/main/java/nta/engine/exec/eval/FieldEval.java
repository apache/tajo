package nta.engine.exec.eval;

import nta.catalog.Column;
import nta.catalog.Schema;
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
	private String tableId;
	private String columnName;
	private int fieldId = -1;
	
	public FieldEval(String tableName, String columnName, DataType domain) {
		super(Type.FIELD);
		this.dataType = domain;
		this.tableId = tableName;
		this.columnName = columnName;
	}
	
	public FieldEval(Column col) {
	  super(Type.FIELD);
	  this.dataType = col.getDataType();
	  this.tableId = col.getTableId();
	  this.columnName = col.getName();
	}

	@Override
	public Datum eval(Schema schema, Tuple tuple, Datum...args) {
	  if(fieldId == -1) {
	    fieldId = schema.getColumn(columnName).getId();
	  }
	  
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
	
	public String getTableId() {
	  return this.tableId;
	}
	
	public String getColumnName() {
	  return this.columnName;
	}

	@Override
	public String getName() {		
		return columnName;
	}
	
	public String toString() {
		return "FIELD("+fieldId+")";
	}
}
