package nta.catalog;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import nta.catalog.exception.AlreadyExistsFieldException;
import nta.catalog.proto.TableProtos.ColumnProto;
import nta.catalog.proto.TableProtos.DataType;
import nta.catalog.proto.TableProtos.SchemaProto;
import nta.catalog.proto.TableProtos.SchemaProtoOrBuilder;
import nta.common.ProtoObject;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class Schema implements ProtoObject<SchemaProto> {
	private SchemaProto proto = SchemaProto.getDefaultInstance();
	private	SchemaProto.Builder builder = null;
	boolean viaProto = false;

	protected volatile int newFieldId = 0;
	protected Map<Integer,Column> fields = null;
	protected Map<String,Integer> fieldsByName = null;

	public Schema() {
		builder = SchemaProto.newBuilder();
	}
	
	public Schema(SchemaProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }

	public Schema(Schema schema) {
		this.newFieldId = schema.newFieldId;
		this.fields = new TreeMap<Integer,Column>(schema.fields);
		this.fieldsByName = new HashMap<String, Integer>(schema.fieldsByName);
	}
	
	public Schema(Column [] columns) {
    this();
    for(Column c : columns) {
      addColumn(c);
    }
  }
	
	public int getColumnNum() {
		initColumns();
		return this.fields.size();
	}

	public Column getColumn(int colId) {
		initColumns();		
		return fields.get(colId);
	}

	public Column getColumn(String colName) {
		initColumns();
		Integer cid = fieldsByName.get(colName);
		return cid != null ? fields.get(cid) : null;
	}
	
	public Collection<Column> getColumns() {
		initColumns();
		return fields.values();
	}
	
	public boolean contains(String colName) {
		initColumns();
		return fieldsByName.containsKey(colName);
	}

	private void initColumns() {
		if (this.fields != null) {
			return;
		}
		SchemaProtoOrBuilder p = viaProto ? proto : builder;
		this.fields = new HashMap<Integer, Column>();
		this.fieldsByName = new HashMap<String, Integer>();
		for(ColumnProto colProto : p.getFieldsList()) {
			newFieldId++;
			fields.put(colProto.getColumnId(), new Column(colProto));
			fieldsByName.put(colProto.getColumnName(), colProto.getColumnId());			
		}
	}

	public synchronized int addColumn(String name, DataType dataType) {
		initColumns();		
		if(fieldsByName.containsKey(name)) {
			throw new AlreadyExistsFieldException(name);
		}
		maybeInitBuilder();		
		int fid = newFieldId();
		Column newCol = new Column(fid, name, dataType);
		fields.put(fid, newCol);
		fieldsByName.put(name, fid);

		return fid;
	}
	
	public synchronized int addColumn(Column column) {
		return addColumn(column.getName(), column.getDataType());		
	}

	private int newFieldId() {
		return newFieldId++;
	}

	// TODO - to be implemented
	@Override
	public boolean equals(Object o) {
		if (o instanceof Schema) {			
			return true;
		}
		return false;
	}

	@Override
	public SchemaProto getProto() {
		mergeLocalToProto();
		proto = viaProto ? proto : builder.build();
		viaProto = true;
		return proto;
	}

	private void maybeInitBuilder() {
		if (viaProto || builder == null) {
			builder = SchemaProto.newBuilder(proto);
		}
		viaProto = false;
	}

	private void mergeLocalToBuilder() {
	  maybeInitBuilder();
	  if (fields != null)
	    builder.clearFields();
	  
		if (this.fields  != null) {			
			for(Column col : fields.values()) {
				builder.addFields(col.getProto());
			}
		}
	}

	private void mergeLocalToProto() {
		if(viaProto) {
			maybeInitBuilder();
		}
		
		mergeLocalToBuilder();
		proto = builder.build();
		viaProto = true;
		
	}
	
	public String toString() {
	  initColumns();
	  StringBuilder sb = new StringBuilder();
	  sb.append("{");
	  for(Column col : fields.values()) {
	    sb.append(col).append(",");
	  }
	  sb.append("}");
	  
	  return sb.toString();
	}
}