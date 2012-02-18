package nta.catalog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nta.catalog.exception.AlreadyExistsFieldException;
import nta.catalog.proto.CatalogProtos.ColumnProto;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.SchemaProto;
import nta.catalog.proto.CatalogProtos.SchemaProtoOrBuilder;
import nta.common.ProtoObject;
import nta.engine.json.GsonCreator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.gson.Gson;
import com.google.gson.annotations.Expose;

/**
 * @author Hyunsik Choi
 */
public class Schema implements ProtoObject<SchemaProto>, Cloneable {
  private static final Log LOG = LogFactory.getLog(Schema.class);
  
	private SchemaProto proto = SchemaProto.getDefaultInstance();
	private	SchemaProto.Builder builder = null;
	boolean viaProto = false;

	@Expose
	protected List<Column> fields = null;
	@Expose
	protected Map<String,Integer> fieldsByName = null;

	public Schema() {
		builder = SchemaProto.newBuilder();
	}
	
	public Schema(SchemaProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }

	public Schema(Schema schema) {
	  this();
		this.fields = new ArrayList<Column>(schema.fields);
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

	public Column getColumn(String colName) {
		initColumns();
		Integer cid = fieldsByName.get(colName);
		return cid != null ? fields.get(cid) : null;
	}
	
	public Column getColumn(int id) {
	  initColumns();
	  return fields.get(id);
	}
	
	public int getColumnId(String colName) {
	  initColumns();
	  return fieldsByName.get(colName);
	}
	
	public Collection<Column> getColumns() {
		initColumns();
		return fields;
	}
	
	public boolean contains(String colName) {
		initColumns();
		return fieldsByName.containsKey(colName);
	}
	
	public void initFromProto() {
		initColumns();
		for (Column col : fields) {
		  col.initFromProto();
		}
	}

	private void initColumns() {
		if (this.fields != null) {
			return;
		}
		SchemaProtoOrBuilder p = viaProto ? proto : builder;
		this.fields = new ArrayList<Column>();
		this.fieldsByName = new HashMap<String, Integer>();
		for(ColumnProto colProto : p.getFieldsList()) {
			fields.add(new Column(colProto));
			fieldsByName.put(colProto.getColumnName(), fields.size() - 1);
		}
	}

	public synchronized Schema addColumn(String name, DataType dataType) {
		initColumns();
		setModified();
		if(fieldsByName.containsKey(name)) {
		  LOG.error("Already exists column " + name);
			throw new AlreadyExistsFieldException(name);
		}
			
		Column newCol = new Column(name, dataType);
		fields.add(newCol);
		fieldsByName.put(name, fields.size() - 1);
		
		return this;
	}
	
	public synchronized void addColumn(Column column) {
		addColumn(column.getQualifiedName(), column.getDataType());		
	}
	
	public synchronized void addColumns(Schema schema) {
    for(Column column : schema.getColumns()) {
      addColumn(column);
    }
  }

	@Override
	public boolean equals(Object o) {
		if (o instanceof Schema) {
		  Schema other = (Schema) o;
		  return getProto().equals(other.getProto());
		}
		return false;
	}
	
  @Override
  public Object clone() throws CloneNotSupportedException {
    Schema schema = (Schema) super.clone();
    initFromProto();
    schema.viaProto = false;
    schema.builder = SchemaProto.newBuilder();
    schema.fields = fields != null ? new ArrayList<Column>(fields) : null;
    schema.fieldsByName = fieldsByName != null ? new HashMap<String, Integer>(
        fieldsByName) : null;

    return schema;
  }

	@Override
	public SchemaProto getProto() {
	  if(!viaProto) {
      mergeLocalToBuilder();
      proto = builder.build();
      viaProto = true;
    }
	  
		return proto;
	}

	private void setModified() {
		viaProto = false;
	}

	private void mergeLocalToBuilder() {
	  if (builder == null) {
	    builder = SchemaProto.newBuilder(proto);
	  } else {	  
	    builder.clearFields();
	  }
	  
		if (this.fields  != null) {			
			for(Column col : fields) {
				builder.addFields(col.getProto());
			}
		}
	}

	public String toString() {
	  initColumns();
	  StringBuilder sb = new StringBuilder();
	  sb.append("{");
	  for(Column col : fields) {
	    sb.append(col).append(",");
	  }
	  sb.append("}");
	  
	  return sb.toString();
	}
	
	public String toJson() {
	  initFromProto();
	  Gson gson = GsonCreator.getInstance();
	  return gson.toJson(this, Schema.class);
		
	}
}