/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.catalog;

import com.google.gson.annotations.Expose;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.catalog.exception.AlreadyExistsFieldException;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos.ColumnProto;
import org.apache.tajo.catalog.proto.CatalogProtos.SchemaProto;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.common.TajoDataTypes.Type;

import java.util.*;

public class Schema implements ProtoObject<SchemaProto>, Cloneable, GsonObject {
  private static final Log LOG = LogFactory.getLog(Schema.class);
	private	SchemaProto.Builder builder = SchemaProto.newBuilder();

	@Expose protected List<Column> fields = null;
	@Expose protected Map<String,Integer> fieldsByName = null;

	public Schema() {
    this.fields = new ArrayList<Column>();
    this.fieldsByName = new TreeMap<String, Integer>();
	}
	
	public Schema(SchemaProto proto) {
    this.fields = new ArrayList<Column>();
    this.fieldsByName = new HashMap<String, Integer>();
    for(ColumnProto colProto : proto.getFieldsList()) {
      fields.add(new Column(colProto));
      fieldsByName.put(colProto.getColumnName(), fields.size() - 1);
    }
  }

	public Schema(Schema schema) {
	  this();
		this.fields.addAll(schema.fields);
		this.fieldsByName.putAll(schema.fieldsByName);
	}
	
	public Schema(Column [] columns) {
    this();
    for(Column c : columns) {
      addColumn(c);
    }
  }
	
	public int getColumnNum() {
		return this.fields.size();
	}

	public Column getColumn(String colName) {
		Integer cid = fieldsByName.get(colName.toLowerCase());
		return cid != null ? fields.get(cid) : null;
	}
	
	public Column getColumnByName(String colName) {
	  for (Column col : fields) {
	    if (col.getColumnName().equals(colName.toLowerCase())) {
	      return col;
	    }
	  }
	  return null;
	}
	
	public Column getColumn(int id) {
	  return fields.get(id);
	}
	
	public int getColumnId(String colName) {
	  return fieldsByName.get(colName.toLowerCase());
	}

  public int getColumnIdByName(String colName) {
    for (Column col : fields) {
      if (col.getColumnName().equals(colName.toLowerCase())) {
        return fieldsByName.get(col.getQualifiedName());
      }
    }
    return -1;
  }
	
	public Collection<Column> getColumns() {
		return fields;
	}
	
	public void alter(int idx, Column column) {
	  this.fields.set(idx, column);
	}
	
	public boolean contains(String colName) {
		return fieldsByName.containsKey(colName.toLowerCase());
	}

  public synchronized Schema addColumn(String name, Type type) {
    return addColumn(name, CatalogUtil.newDataTypeWithoutLen(type));
  }

  public synchronized Schema addColumn(String name, DataType dataType) {
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
    schema.builder = SchemaProto.newBuilder();
    schema.fields = fields != null ? new ArrayList<Column>(fields) : null;
    schema.fieldsByName = fieldsByName != null ? new TreeMap<String, Integer>(fieldsByName) : null;

    return schema;
  }

	@Override
	public SchemaProto getProto() {
    builder.clearFields();
    if (this.fields  != null) {
      for(Column col : fields) {
        builder.addFields(col.getProto());
      }
    }
    return builder.build();
	}

	public String toString() {
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

  @Override
	public String toJson() {
	  return CatalogGsonHelper.toJson(this, Schema.class);
		
	}

  public Column [] toArray() {
    return this.fields.toArray(new Column[this.fields.size()]);
  }
}