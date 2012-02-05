package nta.engine.exec.eval;

import com.google.gson.Gson;
import com.google.gson.annotations.Expose;

import nta.catalog.Column;
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
public class FieldEval extends EvalNode {
	@Expose
	DataType dataType;
	@Expose
	private String columnName;
	@Expose
	private int fieldId = -1;
	
	public FieldEval(String columnName, DataType domain) {
		super(Type.FIELD);
		this.dataType = domain;
		this.columnName = columnName;
	}
	
	public FieldEval(Column col) {
	  super(Type.FIELD);
	  this.dataType = col.getDataType();
	  this.columnName = col.getName();
	}

	@Override
	public Datum eval(Schema schema, Tuple tuple, Datum...args) {
	  if (fieldId == -1) {
	    fieldId = schema.getColumnId(columnName);
	  }
	  
/*		switch(dataType) {
		case BOOLEAN: return tuple.getBoolean(fieldId);
		case BYTE: return tuple.getByte(fieldId);
		case INT: return tuple.getInt(fieldId);
		case LONG: return tuple.getLong(fieldId);
		case FLOAT: return tuple.getFloat(fieldId);
		case DOUBLE: return tuple.getDouble(fieldId);
		case STRING: return tuple.getString(fieldId);
		case BYTES: return tuple.getBytes(fieldId);
		case IPv4: return tuple.getIPv4(fieldId);
		default: throw new InvalidEvalException();
		}*/
	  return tuple.get(fieldId);
	}
	
	@Override
	public DataType getValueType() {
		return dataType;
	}
	
	public String getTableId() {	  
	  return columnName.split("\\.")[0];
	}
	
	public String getColumnName() {
	  return columnName.split("\\.")[1];
	}

	@Override
	public String getName() {
		return columnName;
	}
	
	public String toString() {
	  return columnName + " " + dataType;
	}
	
	public String toJSON() {
	  Gson gson = GsonCreator.getInstance();
    return gson.toJson(this, EvalNode.class);
	}
	
  public boolean equals(Object obj) {
    if (obj instanceof FieldEval) {
      FieldEval other = (FieldEval) obj;

      if (this.type == other.type && this.columnName.equals(other.columnName)
          && this.dataType.equals(other.dataType)) {
        return true;
      }
    }
    return false;
  }
}
