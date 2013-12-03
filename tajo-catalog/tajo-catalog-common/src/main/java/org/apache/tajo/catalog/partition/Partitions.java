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

package org.apache.tajo.catalog.partition;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.exception.AlreadyExistsFieldException;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.util.TUtil;

import java.util.*;

public class Partitions implements ProtoObject<CatalogProtos.PartitionsProto>, Cloneable, GsonObject {

  private static final Log LOG = LogFactory.getLog(Partitions.class);

  @Expose protected CatalogProtos.PartitionsType partitionsType; //required
  @Expose protected List<Column> columns; //required
  @Expose protected int numPartitions; //optional
  @Expose protected List<Specifier> specifiers; //optional
  @Expose protected Map<String, Integer> columnsByQialifiedName = null;
  @Expose protected Map<String, List<Integer>> columnsByName = null;

  private CatalogProtos.PartitionsProto.Builder builder = CatalogProtos.PartitionsProto.newBuilder();

  public Partitions() {
    this.columns = new ArrayList<Column>();
    this.columnsByQialifiedName = new TreeMap<String, Integer>();
    this.columnsByName = new HashMap<String, List<Integer>>();
  }

  public Partitions(Partitions partition) {
    this();
    this.partitionsType = partition.partitionsType;
    this.columns.addAll(partition.columns);
    this.columnsByQialifiedName.putAll(partition.columnsByQialifiedName);
    this.columnsByName.putAll(partition.columnsByName);
    this.numPartitions = partition.numPartitions;
    this.specifiers = partition.specifiers;
  }

  public Partitions(CatalogProtos.PartitionsType partitionsType, Column[] columns, int numPartitions,
                   List<Specifier> specifiers) {
    this();
    this.partitionsType = partitionsType;
    for (Column c : columns) {
      addColumn(c);
    }
    this.numPartitions = numPartitions;
    this.specifiers = specifiers;
  }

  public Partitions(CatalogProtos.PartitionsProto proto) {
    this.partitionsType = proto.getPartitionsType();
    this.columns = new ArrayList<Column>();
    this.columnsByQialifiedName = new HashMap<String, Integer>();
    this.columnsByName = new HashMap<String, List<Integer>>();
    for (CatalogProtos.ColumnProto colProto : proto.getColumnsList()) {
      Column tobeAdded = new Column(colProto);
      columns.add(tobeAdded);
      if (tobeAdded.hasQualifier()) {
        columnsByQialifiedName.put(tobeAdded.getQualifier() + "." + tobeAdded.getColumnName(),
            columns.size() - 1);
      } else {
        columnsByQialifiedName.put(tobeAdded.getColumnName(), columns.size() - 1);
      }
      if (columnsByName.containsKey(tobeAdded.getColumnName())) {
        columnsByName.get(tobeAdded.getColumnName()).add(columns.size() - 1);
      } else {
        columnsByName.put(tobeAdded.getColumnName(), TUtil.newList(columns.size() - 1));
      }
    }
    this.numPartitions = proto.getNumPartitions();
    if(proto.getSpecifiersList() != null) {
      this.specifiers = TUtil.newList();
      for(CatalogProtos.SpecifierProto specifier: proto.getSpecifiersList()) {
        this.specifiers.add(new Specifier(specifier));
      }
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
   * @param force     If true, all columns' qualifiers will be changed. Otherwise,
   *                  only qualified columns' qualifiers will
   *                  be changed.
   */
  public void setQualifier(String qualifier, boolean force) {
    columnsByQialifiedName.clear();

    for (int i = 0; i < getColumnNum(); i++) {
      if (!force && columns.get(i).hasQualifier()) {
        continue;
      }
      columns.get(i).setQualifier(qualifier);
      columnsByQialifiedName.put(columns.get(i).getQualifiedName(), i);
    }
  }

  public int getColumnNum() {
    return this.columns.size();
  }

  public Column getColumn(int id) {
    return columns.get(id);
  }

  public Column getColumnByFQN(String qualifiedName) {
    Integer cid = columnsByQialifiedName.get(qualifiedName.toLowerCase());
    return cid != null ? columns.get(cid) : null;
  }

  public Column getColumnByName(String colName) {
    String normalized = colName.toLowerCase();
    List<Integer> list = columnsByName.get(normalized);

    if (list == null || list.size() == 0) {
      return null;
    }

    if (list.size() == 1) {
      return columns.get(list.get(0));
    } else {
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      for (Integer id : list) {
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append(columns.get(id));
      }
      throw new RuntimeException("Ambiguous Column Name: " + sb.toString());
    }
  }

  public int getColumnId(String qualifiedName) {
    return columnsByQialifiedName.get(qualifiedName.toLowerCase());
  }

  public int getColumnIdByName(String colName) {
    for (Column col : columns) {
      if (col.getColumnName().equals(colName.toLowerCase())) {
        return columnsByQialifiedName.get(col.getQualifiedName());
      }
    }
    return -1;
  }

  public List<Column> getColumns() {
    return ImmutableList.copyOf(columns);
  }

  public void setColumns(List<Column> columns) {
    this.columns = columns;
  }

  public boolean contains(String colName) {
    return columnsByQialifiedName.containsKey(colName.toLowerCase());

  }

  public boolean containsAll(Collection<Column> columns) {
    return columns.containsAll(columns);
  }

  public synchronized Partitions addColumn(String name, TajoDataTypes.Type type) {
    if (type == TajoDataTypes.Type.CHAR) {
      return addColumn(name, CatalogUtil.newDataTypeWithLen(type, 1));
    }
    return addColumn(name, CatalogUtil.newSimpleDataType(type));
  }

  public synchronized Partitions addColumn(String name, TajoDataTypes.Type type, int length) {
    return addColumn(name, CatalogUtil.newDataTypeWithLen(type, length));
  }

  public synchronized Partitions addColumn(String name, TajoDataTypes.DataType dataType) {
    String normalized = name.toLowerCase();
    if (columnsByQialifiedName.containsKey(normalized)) {
      LOG.error("Already exists column " + normalized);
      throw new AlreadyExistsFieldException(normalized);
    }

    Column newCol = new Column(normalized, dataType);
    columns.add(newCol);
    columnsByQialifiedName.put(newCol.getQualifiedName(), columns.size() - 1);
    columnsByName.put(newCol.getColumnName(), TUtil.newList(columns.size() - 1));

    return this;
  }

  public synchronized void addColumn(Column column) {
    addColumn(column.getQualifiedName(), column.getDataType());
  }

  public synchronized void addColumns(Partitions schema) {
    for (Column column : schema.getColumns()) {
      addColumn(column);
    }
  }

  public synchronized void addSpecifier(Specifier specifier) {
    if(specifiers == null)
      specifiers = TUtil.newList();

    specifiers.add(specifier);
  }

  public CatalogProtos.PartitionsType getPartitionsType() {
    return partitionsType;
  }

  public void setPartitionsType(CatalogProtos.PartitionsType partitionsType) {
    this.partitionsType = partitionsType;
  }

  public int getNumPartitions() {
    return numPartitions;
  }

  public void setNumPartitions(int numPartitions) {
    this.numPartitions = numPartitions;
  }

  public List<Specifier> getSpecifiers() {
    return specifiers;
  }

  public void setSpecifiers(List<Specifier> specifiers) {
    this.specifiers = specifiers;
  }

  public Map<String, Integer> getColumnsByQialifiedName() {
    return columnsByQialifiedName;
  }

  public void setColumnsByQialifiedName(Map<String, Integer> columnsByQialifiedName) {
    this.columnsByQialifiedName = columnsByQialifiedName;
  }

  public Map<String, List<Integer>> getColumnsByName() {
    return columnsByName;
  }

  public void setColumnsByName(Map<String, List<Integer>> columnsByName) {
    this.columnsByName = columnsByName;
  }

  public boolean equals(Object o) {
    if (o instanceof Partitions) {
      Partitions other = (Partitions) o;
      return getProto().equals(other.getProto());
    }
    return false;
  }

  public Object clone() throws CloneNotSupportedException {
    Partitions clone = (Partitions) super.clone();
    clone.builder = CatalogProtos.PartitionsProto.newBuilder();
    clone.setPartitionsType(this.partitionsType);
    clone.setColumns(this.columns);
    clone.setNumPartitions(this.numPartitions);
    clone.specifiers = new ArrayList<Specifier>(this.specifiers);

    return clone;
  }

  @Override
  public CatalogProtos.PartitionsProto getProto() {
    if (builder == null) {
      builder = CatalogProtos.PartitionsProto.newBuilder();
    }
    if (this.partitionsType != null) {
      builder.setPartitionsType(this.partitionsType);
    }
    builder.clearColumns();
    if (this.columns != null) {
      for (Column col : columns) {
        builder.addColumns(col.getProto());
      }
    }
    builder.setNumPartitions(numPartitions);

    if (this.specifiers != null) {
      for(Specifier specifier: specifiers) {
        builder.addSpecifiers(specifier.getProto());
      }
    }
    return builder.build();
  }

  public String toString() {
    Gson gson = new GsonBuilder().setPrettyPrinting().
        excludeFieldsWithoutExposeAnnotation().create();
    return gson.toJson(this);
  }

  @Override
  public String toJson() {
    return CatalogGsonHelper.toJson(this, Partitions.class);

  }

  public Column[] toArray() {
    return this.columns.toArray(new Column[this.columns.size()]);
  }

}