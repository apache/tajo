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

import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.Expose;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.exception.AlreadyExistsFieldException;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos.ColumnProto;
import org.apache.tajo.catalog.proto.CatalogProtos.SchemaProto;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.util.TUtil;

import java.util.*;

public class Schema implements ProtoObject<SchemaProto>, Cloneable, GsonObject {
  private static final Log LOG = LogFactory.getLog(Schema.class);
	private	SchemaProto.Builder builder = SchemaProto.newBuilder();

	@Expose protected List<Column> fields = null;
	@Expose protected Map<String, Integer> fieldsByQialifiedName = null;
  @Expose protected Map<String, List<Integer>> fieldsByName = null;

	public Schema() {
    this.fields = new ArrayList<Column>();
    this.fieldsByQialifiedName = new TreeMap<String, Integer>();
    this.fieldsByName = new HashMap<String, List<Integer>>();
	}
	
	public Schema(SchemaProto proto) {
    this.fields = new ArrayList<Column>();
    this.fieldsByQialifiedName = new HashMap<String, Integer>();
    this.fieldsByName = new HashMap<String, List<Integer>>();
    for(ColumnProto colProto : proto.getFieldsList()) {
      Column tobeAdded = new Column(colProto);
      fields.add(tobeAdded);
      if (tobeAdded.hasQualifier()) {
        fieldsByQialifiedName.put(tobeAdded.getQualifier() + "." + tobeAdded.getColumnName(), fields.size() - 1);
      } else {
        fieldsByQialifiedName.put(tobeAdded.getColumnName(), fields.size() - 1);
      }
      if (fieldsByName.containsKey(tobeAdded.getColumnName())) {
        fieldsByName.get(tobeAdded.getColumnName()).add(fields.size() - 1);
      } else {
        fieldsByName.put(tobeAdded.getColumnName(), TUtil.newList(fields.size() - 1));
      }
    }
  }

	public Schema(Schema schema) {
	  this();
		this.fields.addAll(schema.fields);
		this.fieldsByQialifiedName.putAll(schema.fieldsByQialifiedName);
    this.fieldsByName.putAll(schema.fieldsByName);
	}
	
	public Schema(Column [] columns) {
    this();
    for(Column c : columns) {
      addColumn(c);
    }
  }

  /**
   * Set a qualifier to this schema.
   * This changes the qualifier of all columns except for not-qualified columns.
   *
   * @param qualifier The qualifier
   */
  public void setQualifier(String qualifier) {
    setQualifier(qualifier, false);
  }

  /**
   * Set a qualifier to this schema. This changes the qualifier of all columns if force is true.
   * Otherwise, it changes the qualifier of all columns except for non-qualified columns
   *
   * @param qualifier The qualifier
   * @param force If true, all columns' qualifiers will be changed. Otherwise, only qualified columns' qualifiers will
   *              be changed.
   */
  public void setQualifier(String qualifier, boolean force) {
    fieldsByQialifiedName.clear();

    for (int i = 0; i < getColumnNum(); i++) {
      if (!force && fields.get(i).hasQualifier()) {
        continue;
      }
      fields.get(i).setQualifier(qualifier);
      fieldsByQialifiedName.put(fields.get(i).getQualifiedName(), i);
    }
  }
	
	public int getColumnNum() {
		return this.fields.size();
	}

  public Column getColumn(int id) {
    return fields.get(id);
  }

	public Column getColumnByFQN(String qualifiedName) {
		Integer cid = fieldsByQialifiedName.get(qualifiedName.toLowerCase());
		return cid != null ? fields.get(cid) : null;
	}
	
	public Column getColumnByName(String colName) {
    String normalized = colName.toLowerCase();
	  List<Integer> list = fieldsByName.get(normalized);

    if (list == null || list.size() == 0) {
      return null;
    }

    if (list.size() == 1) {
      return fields.get(list.get(0));
    } else {
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      for (Integer id : list) {
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append(fields.get(id));
      }
      throw new RuntimeException("Ambiguous Column Name: " + sb.toString());
    }
	}
	
	public int getColumnId(String qualifiedName) {
	  return fieldsByQialifiedName.get(qualifiedName.toLowerCase());
	}

  public int getColumnIdByName(String colName) {
    for (Column col : fields) {
      if (col.getColumnName().equals(colName.toLowerCase())) {
        return fieldsByQialifiedName.get(col.getQualifiedName());
      }
    }
    return -1;
  }
	
	public List<Column> getColumns() {
		return ImmutableList.copyOf(fields);
	}
	
	public boolean contains(String colName) {
		return fieldsByQialifiedName.containsKey(colName.toLowerCase());

	}

  public boolean containsAll(Collection<Column> columns) {
    return fields.containsAll(columns);
  }

  public synchronized Schema addColumn(String name, Type type) {
    if (type == Type.CHAR) {
      return addColumn(name, CatalogUtil.newDataTypeWithLen(type, 1));
    }
    return addColumn(name, CatalogUtil.newSimpleDataType(type));
  }

  public synchronized Schema addColumn(String name, Type type, int length) {
    return addColumn(name, CatalogUtil.newDataTypeWithLen(type, length));
  }

  public synchronized Schema addColumn(String name, DataType dataType) {
		String normalized = name.toLowerCase();
		if(fieldsByQialifiedName.containsKey(normalized)) {
		  LOG.error("Already exists column " + normalized);
			throw new AlreadyExistsFieldException(normalized);
		}
			
		Column newCol = new Column(normalized, dataType);
		fields.add(newCol);
		fieldsByQialifiedName.put(newCol.getQualifiedName(), fields.size() - 1);
    fieldsByName.put(newCol.getColumnName(), TUtil.newList(fields.size() - 1));
		
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
  public Object clone() {
    Schema schema = new Schema(toArray());
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