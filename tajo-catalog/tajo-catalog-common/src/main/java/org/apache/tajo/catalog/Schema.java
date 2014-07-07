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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.Expose;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.exception.AlreadyExistsFieldException;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos;
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
	@Expose protected Map<String, Integer> fieldsByQualifiedName = null;
  @Expose protected Map<String, List<Integer>> fieldsByName = null;

	public Schema() {
    init();
	}
	
	public Schema(SchemaProto proto) {
    this.fields = new ArrayList<Column>();
    this.fieldsByQualifiedName = new HashMap<String, Integer>();
    this.fieldsByName = new HashMap<String, List<Integer>>();
    for(ColumnProto colProto : proto.getFieldsList()) {
      Column tobeAdded = new Column(colProto);
      fields.add(tobeAdded);
      if (tobeAdded.hasQualifier()) {
        fieldsByQualifiedName.put(tobeAdded.getQualifier() + "." + tobeAdded.getSimpleName(), fields.size() - 1);
      } else {
        fieldsByQualifiedName.put(tobeAdded.getSimpleName(), fields.size() - 1);
      }
      if (fieldsByName.containsKey(tobeAdded.getSimpleName())) {
        fieldsByName.get(tobeAdded.getSimpleName()).add(fields.size() - 1);
      } else {
        fieldsByName.put(tobeAdded.getSimpleName(), TUtil.newList(fields.size() - 1));
      }
    }
  }

	public Schema(Schema schema) {
	  this();
		this.fields.addAll(schema.fields);
		this.fieldsByQualifiedName.putAll(schema.fieldsByQualifiedName);
    this.fieldsByName.putAll(schema.fieldsByName);
	}

	public Schema(Column [] columns) {
    init();
    for(Column c : columns) {
      addColumn(c);
    }
  }

  private void init() {
    this.fields = new ArrayList<Column>();
    this.fieldsByQualifiedName = new HashMap<String, Integer>();
    this.fieldsByName = new HashMap<String, List<Integer>>();
  }

  /**
   * Set a qualifier to this schema.
   * This changes the qualifier of all columns except for not-qualified columns.
   *
   * @param qualifier The qualifier
   */
  public void setQualifier(String qualifier) {
    fieldsByQualifiedName.clear();
    for (int i = 0; i < size(); i++) {
      Column column = fields.get(i);
      fields.set(i, new Column(qualifier + "." + column.getSimpleName(), column.getDataType()));
      fieldsByQualifiedName.put(fields.get(i).getQualifiedName(), i);
    }
  }
	
	public int size() {
		return this.fields.size();
	}

  public Column getColumn(int id) {
    return fields.get(id);
  }

  public Column getColumn(Column column) {
    if (!contains(column)) {
      return null;
    }
    if (column.hasQualifier()) {
      return fields.get(fieldsByQualifiedName.get(column.getQualifiedName()));
    } else {
      return fields.get(fieldsByName.get(column.getSimpleName()).get(0));
    }
  }

  /**
   * Get a column by a given name.
   *
   * @param name The column name to be found.
   * @return The column matched to a given column name.
   */
  public Column getColumn(String name) {
    String [] parts = name.split("\\.");
    // Some of the string can includes database name and table name and column name.
    // For example, it can be 'default.table1.id'.
    // Therefore, spilt string array length can be 3.
    if (parts.length >= 2) {
      return getColumnByQName(name);
    } else {
      return getColumnByName(name);
    }
  }

  /**
   * Find a column by a qualified name (e.g., table1.col1).
   *
   * @param qualifiedName The qualified name
   * @return The Column matched to a given qualified name
   */
  private Column getColumnByQName(String qualifiedName) {
		Integer cid = fieldsByQualifiedName.get(qualifiedName);
		return cid != null ? fields.get(cid) : null;
	}

  /**
   * Find a column by a name (e.g., col1).
   * The same name columns can be exist in a schema. For example, table1.col1 and table2.col1 coexist in a schema.
   * In this case, it will throw {@link java.lang.RuntimeException}. But, it occurs rarely because all column names
   * except for alias have a qualified form.
   *
   * @param columnName The column name without qualifier
   * @return The Column matched to a given name.
   */
	private Column getColumnByName(String columnName) {
    String normalized = columnName;
	  List<Integer> list = fieldsByName.get(normalized);

    if (list == null || list.size() == 0) {
      return null;
    }

    if (list.size() == 1) {
      return fields.get(list.get(0));
    } else {
      throw throwAmbiguousFieldException(list);
    }
	}

  private RuntimeException throwAmbiguousFieldException(Collection<Integer> idList) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Integer id : idList) {
      if (first) {
        first = false;
      } else {
        sb.append(", ");
      }
      sb.append(fields.get(id));
    }
    throw new RuntimeException("Ambiguous Column Name Access: " + sb.toString());
  }
	
	public int getColumnId(String name) {
    String [] parts = name.split("\\.");
    if (parts.length == 2 || parts.length == 3) {
      if (fieldsByQualifiedName.containsKey(name)) {
        return fieldsByQualifiedName.get(name);
      } else {
        return -1;
      }
    } else {
      List<Integer> list = fieldsByName.get(name);
      if (list == null) {
        return -1;
      } else  if (list.size() == 1) {
        return fieldsByName.get(name).get(0);
      } else if (list.size() == 0) {
        return -1;
      } else { // if list.size > 2
        throw throwAmbiguousFieldException(list);
      }
    }
	}

  public int getColumnIdByName(String colName) {
    for (Column col : fields) {
      if (col.getSimpleName().equals(colName)) {
        String qualifiedName = col.getQualifiedName();
        return fieldsByQualifiedName.get(qualifiedName);
      }
    }
    return -1;
  }
	
	public List<Column> getColumns() {
		return ImmutableList.copyOf(fields);
	}

  public boolean contains(String name) {
    if (fieldsByQualifiedName.containsKey(name)) {
      return true;
    }
    if (fieldsByName.containsKey(name)) {
      if (fieldsByName.get(name).size() > 1) {
        throw new RuntimeException("Ambiguous Column name");
      }
      return true;
    }

    return false;
  }

  public boolean contains(Column column) {
    if (column.hasQualifier()) {
      return fieldsByQualifiedName.containsKey(column.getQualifiedName());
    } else {
      if (fieldsByName.containsKey(column.getSimpleName())) {
        int num = fieldsByName.get(column.getSimpleName()).size();
        if (num == 0) {
          throw new IllegalStateException("No such column name: " + column.getSimpleName());
        }
        if (num > 1) {
          throw new RuntimeException("Ambiguous column name: " + column.getSimpleName());
        }
        return true;
      }
      return false;
    }
  }
	
	public boolean containsByQualifiedName(String qualifiedName) {
		return fieldsByQualifiedName.containsKey(qualifiedName);
	}

  public boolean containsByName(String colName) {
    return fieldsByName.containsKey(colName);
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
		String normalized = name;
		if(fieldsByQualifiedName.containsKey(normalized)) {
		  LOG.error("Already exists column " + normalized);
			throw new AlreadyExistsFieldException(normalized);
		}
			
		Column newCol = new Column(normalized, dataType);
		fields.add(newCol);
		fieldsByQualifiedName.put(newCol.getQualifiedName(), fields.size() - 1);
    fieldsByName.put(newCol.getSimpleName(), TUtil.newList(fields.size() - 1));
		
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
  public int hashCode() {
    return Objects.hashCode(fields, fieldsByQualifiedName, fieldsByName);
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
    Schema schema = null;

    schema = (Schema) super.clone();
    schema.builder = CatalogProtos.SchemaProto.newBuilder();
    schema.init();
    for(Column column: this.fields) {
      schema.addColumn(column);
    }
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
	  sb.append("{(").append(size()).append(") ");
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