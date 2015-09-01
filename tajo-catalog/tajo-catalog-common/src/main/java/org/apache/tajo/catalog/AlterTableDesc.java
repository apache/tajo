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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.partition.PartitionDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.util.KeyValueSet;

import java.net.URI;

import static org.apache.tajo.catalog.proto.CatalogProtos.AlterTableDescProto;


public class AlterTableDesc implements ProtoObject<AlterTableDescProto>, GsonObject, Cloneable {
  @Expose
  protected AlterTableType alterTableType; //required
  @Expose
  protected String tableName;  // required
  @Expose
  protected String newTableName;  // optional
  @Expose
  protected String columnName;   //optional
  @Expose
  protected String newColumnName; //optional
  @Expose
  protected Column addColumn = null; //optional
  @Expose
  protected PartitionDesc partitionDesc; //optional
  @Expose
  protected KeyValueSet properties;
  @Expose
  protected URI newTablePath; // optional

  public AlterTableDesc() {
    init();
  }

  private void init() {
    this.properties = new KeyValueSet();
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getNewTableName() {
    return newTableName;
  }

  public void setNewTableName(String newTableName) {
    this.newTableName = newTableName;
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public String getNewColumnName() {
    return newColumnName;
  }

  public void setNewColumnName(String newColumnName) {
    this.newColumnName = newColumnName;
  }

  public Column getAddColumn() {
    return addColumn;
  }

  public void setAddColumn(Column addColumn) {
    this.addColumn = addColumn;
  }

  public AlterTableType getAlterTableType() {
    return alterTableType;
  }

  public void setAlterTableType(AlterTableType alterTableType) {
    this.alterTableType = alterTableType;
  }

  public PartitionDesc getPartitionDesc() { return partitionDesc; }

  public void setPartitionDesc(PartitionDesc partitionDesc) { this.partitionDesc = partitionDesc; }

  public void setProperties(KeyValueSet properties) {
    this.properties = properties;
  }

  public KeyValueSet getProperties() {
    return properties;
  }

  public void setProperty(String key, String value) {
    this.properties.set(key, value);
  }

  public String getProperty(String key) {
    return this.properties.get(key);
  }

  public void setNewTablePath(Path path) {
    newTablePath = path.toUri();
  }

  public URI getNewTablePath() {
    return newTablePath;
  }

  @Override
  public String toString() {
    Gson gson = new GsonBuilder().setPrettyPrinting().
        excludeFieldsWithoutExposeAnnotation().create();
    return gson.toJson(this);
  }

  @Override
  public AlterTableDesc clone() throws CloneNotSupportedException {
    AlterTableDesc newAlter = (AlterTableDesc) super.clone();
    newAlter.alterTableType = alterTableType;
    newAlter.tableName = tableName;
    newAlter.newTableName = newTableName;
    newAlter.columnName = newColumnName;
    newAlter.addColumn = addColumn;
    newAlter.partitionDesc = partitionDesc;
    newAlter.properties = (KeyValueSet)properties.clone();
    newAlter.newTablePath = URI.create(newTablePath.toString());
    return newAlter;
  }

  @Override
  public String toJson() {
    return CatalogGsonHelper.toJson(this, AlterTableDesc.class);
  }

  @Override
  public AlterTableDescProto getProto() {
    AlterTableDescProto.Builder builder = AlterTableDescProto.newBuilder();

    if (null != this.tableName) {
      builder.setTableName(this.tableName);
    }
    if (null != this.newTableName) {
      builder.setNewTableName(this.newTableName);
    }
    if (null != this.columnName && null != this.newColumnName) {
      final CatalogProtos.AlterColumnProto.Builder alterColumnBuilder = CatalogProtos.AlterColumnProto.newBuilder();
      alterColumnBuilder.setOldColumnName(this.columnName);
      alterColumnBuilder.setNewColumnName(this.newColumnName);
      builder.setAlterColumnName(alterColumnBuilder.build());
    }
    if (null != this.addColumn) {
      builder.setAddColumn(addColumn.getProto());
    }
    if (null != this.properties) {
      builder.setParams(properties.getProto());
    }

    switch (alterTableType) {
      case RENAME_TABLE:
        builder.setAlterTableType(CatalogProtos.AlterTableType.RENAME_TABLE);
        break;
      case RENAME_COLUMN:
        builder.setAlterTableType(CatalogProtos.AlterTableType.RENAME_COLUMN);
        break;
      case ADD_COLUMN:
        builder.setAlterTableType(CatalogProtos.AlterTableType.ADD_COLUMN);
        break;
      case SET_PROPERTY:
        builder.setAlterTableType(CatalogProtos.AlterTableType.SET_PROPERTY);
        break;
      case ADD_PARTITION:
        builder.setAlterTableType(CatalogProtos.AlterTableType.ADD_PARTITION);
        break;
      case DROP_PARTITION:
        builder.setAlterTableType(CatalogProtos.AlterTableType.DROP_PARTITION);
        break;
      default:
    }

    if (null != this.partitionDesc) {
      builder.setPartitionDesc(partitionDesc.getProto());
    }

    if (null != this.newTablePath) {
      builder.setNewTablePath(newTablePath.toString());
    }

    return builder.build();
  }

}
