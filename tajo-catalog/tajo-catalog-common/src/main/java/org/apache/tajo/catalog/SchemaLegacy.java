/*
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

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.SchemaUtil.ColumnVisitor;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos.ColumnProto;
import org.apache.tajo.catalog.proto.CatalogProtos.SchemaProto;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.exception.DuplicateColumnException;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.util.StringUtils;

import javax.annotation.Nullable;
import java.util.*;

import static com.google.common.collect.Collections2.transform;

@Deprecated
public class SchemaLegacy implements Schema, ProtoObject<SchemaProto>, Cloneable, GsonObject {

	@Expose protected List<Column> fields = null;
	@Expose protected Map<String, Integer> fieldsByQualifiedName = null;
  @Expose protected Map<String, List<Integer>> fieldsByName = null;

	public SchemaLegacy() {
    init();
	}

  /**
   * This Schema constructor restores a serialized schema into in-memory Schema structure.
   * A serialized schema is an ordered list in depth-first order over a nested schema.
   * This constructor transforms the list into a tree-like structure.
   *
   * @param proto
   */
	public SchemaLegacy(SchemaProto proto) {
    init();

    Collection<Column> toBeAdded = transform(proto.getFieldsList(), new Function<ColumnProto, Column>() {
      @Override
      public Column apply(@Nullable ColumnProto proto) {
        return new Column(proto);
      }
    });

    for (Column c : toBeAdded) {
      addColumn(c);
    }
  }

	public SchemaLegacy(Schema schema) {
	  new SchemaLegacy(schema.getRootColumns());
	}

  public SchemaLegacy(Column [] columns) {
    init();

    for(Column c : columns) {
      addColumn(c);
    }
  }

	public SchemaLegacy(Iterable<Column> columns) {
    init();

    for(Column c : columns) {
      addColumn(c);
    }
  }

  private void init() {
    this.fields = new ArrayList<>();
    this.fieldsByQualifiedName = new HashMap<>();
    this.fieldsByName = new HashMap<>();
  }

  /**
   * Set a qualifier to this schema.
   * This changes the qualifier of all columns except for not-qualified columns.
   *
   * @param qualifier The qualifier
   */
  @Override
  public void setQualifier(String qualifier) {
    // only change root fields, and must keep each nested field simple name
    List<Column> columns = getRootColumns();

    fields.clear();
    fieldsByQualifiedName.clear();
    fieldsByName.clear();

    Column newColumn;
    for (Column c : columns) {
      newColumn = new Column(qualifier + "." + c.getSimpleName(), c.type);
      addColumn(newColumn);
    }
  }

  @Override
	public int size() {
		return this.fields.size();
	}

  @Override
  public Column getColumn(int id) {
    return fields.get(id);
  }

  @Override
  public Column getColumn(Column column) {
    int idx = getIndex(column);
    return idx >= 0 ? fields.get(idx) : null;
  }

  public int getIndex(Column column) {
    if (!contains(column)) {
      return -1;
    }

    if (column.hasQualifier()) {
      return fieldsByQualifiedName.get(column.getQualifiedName());
    } else {
      return fieldsByName.get(column.getSimpleName()).get(0);
    }
  }

  /**
   * Get a column by a given name.
   *
   * @param name The column name to be found.
   * @return The column matched to a given column name.
   */
  @Override
  public Column getColumn(String name) {

    if (NestedPathUtil.isPath(name)) {

      // TODO - to be refactored
      if (fieldsByQualifiedName.containsKey(name)) {
        Column flattenColumn = fields.get(fieldsByQualifiedName.get(name));
        if (flattenColumn != null) {
          return flattenColumn;
        }
      }

      String [] paths = name.split(NestedPathUtil.PATH_DELIMITER);
      Column column = getColumn(paths[0]);
      if (column == null) {
        return null;
      }
      Column actualColumn = NestedPathUtil.lookupPath(column, paths);

      Column columnPath = new Column(
          column.getQualifiedName() + NestedPathUtil.makePath(paths, 1),
          actualColumn.type);

      return columnPath;
    } else {
      String[] parts = name.split("\\.");
      // Some of the string can includes database name and table name and column name.
      // For example, it can be 'default.table1.id'.
      // Therefore, spilt string array length can be 3.
      if (parts.length >= 2) {
        return getColumnByQName(name);
      } else {
        return getColumnByName(name);
      }
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
   * In this case, it will throw {@link RuntimeException}. But, it occurs rarely because all column names
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

  @Override
	public int getColumnId(String name) {
    // if the same column exists, immediately return that column.
    if (fieldsByQualifiedName.containsKey(name)) {
      return fieldsByQualifiedName.get(name);
    }

    // The following is some workaround code.
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

  @Override
  public int getColumnIdByName(String colName) {
    for (Column col : fields) {
      if (col.getSimpleName().equals(colName)) {
        String qualifiedName = col.getQualifiedName();
        return fieldsByQualifiedName.get(qualifiedName);
      }
    }
    return -1;
  }

  /**
   * Get root columns, meaning all columns except for nested fields.
   *
   * @return A list of root columns
   */
  @Override
	public List<Column> getRootColumns() {
		return ImmutableList.copyOf(fields);
	}

  /**
   * Get all columns, including all nested fields
   *
   * @return A list of all columns
   */
  @Override
  public List<Column> getAllColumns() {
    final List<Column> columnList = new ArrayList<>();

    SchemaUtil.visitSchema(this, new ColumnVisitor() {
      @Override
      public void visit(int depth, List<String> path, Column column) {
        if (path.size() > 0) {
          String parentPath = StringUtils.join(path, NestedPathUtil.PATH_DELIMITER);
          String currentPath = parentPath + NestedPathUtil.PATH_DELIMITER + column.getSimpleName();
          columnList.add(new Column(currentPath, column.getTypeDesc()));
        } else {
          columnList.add(column);
        }
      }
    });

    return columnList;
  }

  @Override
  public boolean contains(String name) {
    // TODO - It's a hack
    if (NestedPathUtil.isPath(name)) {
      return (getColumn(name) != null);
    }

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

  @Override
  public boolean contains(Column column) {
    // TODO - It's a hack
    if (NestedPathUtil.isPath(column.getQualifiedName())) {
      return (getColumn(column.getQualifiedName()) != null);
    }

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

  @Override
	public boolean containsByQualifiedName(String qualifiedName) {
		return fieldsByQualifiedName.containsKey(qualifiedName);
	}

  @Override
  public boolean containsByName(String colName) {
    return fieldsByName.containsKey(colName);
  }

  @Override
  public boolean containsAll(Collection<Column> columns) {
    boolean containFlag = true;

    for (Column c :columns) {
      if (NestedPathUtil.isPath(c.getSimpleName())) {
        if (contains(c.getQualifiedName())) {
          containFlag &= true;
        } else {
          String[] paths = c.getQualifiedName().split("/");
          boolean existRootPath = contains(paths[0]);
          boolean existLeafPath = getColumn(c.getSimpleName()) != null;
          containFlag &= existRootPath && existLeafPath;
        }
      } else {
        containFlag &= fields.contains(c);
      }
    }

    return containFlag;
  }

  /**
   * Return TRUE if any column in <code>columns</code> is included in this schema.
   *
   * @param columns Columns to be checked
   * @return true if any column in <code>columns</code> is included in this schema.
   *         Otherwise, false.
   */
  @Override
  public boolean containsAny(Collection<Column> columns) {
    for (Column column : columns) {
      if (contains(column)) {
        return true;
      }
    }
    return false;
  }

  private SchemaLegacy addColumn(String name, org.apache.tajo.type.Type type) {
    String normalized = name;
    if(fieldsByQualifiedName.containsKey(normalized)) {
      throw new TajoRuntimeException(new DuplicateColumnException(normalized));
    }

    Column newCol = new Column(normalized, type);
    fields.add(newCol);
    fieldsByQualifiedName.put(newCol.getQualifiedName(), fields.size() - 1);
    List<Integer> inputList = new ArrayList<>();
    inputList.add(fields.size() - 1);
    fieldsByName.put(newCol.getSimpleName(), inputList);

    return this;
  }

	private synchronized void addColumn(Column column) {
		addColumn(column.getQualifiedName(), column.type);
	}

  @Override
  public int hashCode() {
    return Objects.hashCode(fields, fieldsByQualifiedName, fieldsByName);
  }

  @Override
	public boolean equals(Object o) {
		if (o instanceof SchemaLegacy) {
		  SchemaLegacy other = (SchemaLegacy) o;
		  return getProto().equals(other.getProto());
		}
		return false;
	}
	
  @Override
  public Object clone() throws CloneNotSupportedException {
    SchemaLegacy schema = (SchemaLegacy) super.clone();
    schema.init();

    for(Column column: this.fields) {
      schema.addColumn(column);
    }
    return schema;
  }

	@Override
	public SchemaProto getProto() {
    SchemaProto.Builder builder = SchemaProto.newBuilder();
    builder.addAllFields(Iterables.transform(getRootColumns(), new Function<Column, ColumnProto>() {
      @Override
      public ColumnProto apply(@Nullable Column column) {
        return column.getProto();
      }
    }));
    return builder.build();
	}

  @Override
	public String toString() {
	  StringBuilder sb = new StringBuilder();
	  sb.append("{(").append(size()).append(") ");
	  int i = 0;
	  for(Column col : fields) {
	    sb.append(col);
	    if (i < fields.size() - 1) {
	      sb.append(", ");
	    }
	    i++;
	  }
	  sb.append("}");
	  
	  return sb.toString();
	}

  @Override
	public String toJson() {
	  return CatalogGsonHelper.toJson(this, SchemaLegacy.class);
	}

  @Override
  public Column [] toArray() {
    return this.fields.toArray(new Column[this.fields.size()]);
  }
}