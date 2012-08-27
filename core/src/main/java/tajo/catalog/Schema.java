package tajo.catalog;

import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import tajo.catalog.exception.AlreadyExistsFieldException;
import tajo.catalog.proto.CatalogProtos.ColumnProto;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.SchemaProto;
import tajo.catalog.proto.CatalogProtos.SchemaProtoOrBuilder;
import tajo.common.ProtoObject;
import tajo.engine.json.GsonCreator;

import java.util.*;

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
		Integer cid = fieldsByName.get(colName.toLowerCase());
		return cid != null ? fields.get(cid) : null;
	}
	
	public Column getColumnByName(String colName) {
	  initColumns();
	  for (Column col : fields) {
	    if (col.getColumnName().equals(colName.toLowerCase())) {
	      return col;
	    }
	  }
	  return null;
	}
	
	public Column getColumn(int id) {
	  initColumns();
	  return fields.get(id);
	}
	
	public int getColumnId(String colName) {
	  initColumns();
	  return fieldsByName.get(colName.toLowerCase());
	}

  public int getColumnIdByName(String colName) {
    initColumns();
    for (Column col : fields) {
      if (col.getColumnName().equals(colName.toLowerCase())) {
        return fieldsByName.get(col.getQualifiedName());
      }
    }
    return -1;
  }
	
	public Collection<Column> getColumns() {
		initColumns();
		return fields;
	}
	
	public void alter(int idx, Column column) {
	  initColumns();
	  this.fields.set(idx, column);
	}
	
	public boolean contains(String colName) {
		initColumns();
		return fieldsByName.containsKey(colName.toLowerCase());
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
		String lowcased = name.toLowerCase();
		if(fieldsByName.containsKey(lowcased)) {
		  LOG.error("Already exists column " + lowcased);
			throw new AlreadyExistsFieldException(lowcased);
		}
			
		Column newCol = new Column(lowcased, dataType);
		fields.add(newCol);
		fieldsByName.put(lowcased, fields.size() - 1);
		
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
	  int i = 0;
	  for(Column col : fields) {
	    sb.append(col);
	    if (i < fields.size() - 1) {
	      sb.append(",");
	    }
	    i++;
	  }
	  sb.append("}");
	  
	  return sb.toString();
	}
	
	public String toJson() {
	  initFromProto();
	  Gson gson = GsonCreator.getInstance();
	  return gson.toJson(this, Schema.class);
		
	}

  public Column [] toArray() {
    return this.fields.toArray(new Column[this.fields.size()]);
  }
}