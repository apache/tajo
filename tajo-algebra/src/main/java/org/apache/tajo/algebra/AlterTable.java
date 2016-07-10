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

package org.apache.tajo.algebra;


import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.tajo.util.TUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlterTable extends Expr {

  @Expose @SerializedName("OldTableName")
  private String tableName;
  @Expose @SerializedName("NewTableName")
  private String newTableName;
  @Expose @SerializedName("OldColumnName")
  private String columnName;
  @Expose @SerializedName("NewColumnName")
  private String newColumnName;
  @Expose @SerializedName("NewColumnDef")
  private ColumnDefinition addNewColumn;
  @Expose @SerializedName("AlterTableType")
  private AlterTableOpType alterTableOpType;
  @Expose @SerializedName("TableProperties")
  private Map<String, String> params;
  @Expose @SerializedName("UnsetPropertyKeys")
  private List<String> unsetPropertyKeys;

  @Expose @SerializedName("Columns")
  ColumnReferenceExpr [] columns;
  @Expose @SerializedName("Values")
  private Expr[] values;
  @Expose @SerializedName("location")
  private String location;

  @Expose @SerializedName("IsPurge")
  private boolean purge;

  @Expose @SerializedName("IfNotExists")
  private boolean ifNotExists;
  @Expose @SerializedName("IfExists")
  private boolean ifExists;

  public AlterTable(final String tableName) {
    super(OpType.AlterTable);
    this.tableName = tableName;
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

  public ColumnDefinition getAddNewColumn() {
    return addNewColumn;
  }

  public void setAddNewColumn(ColumnDefinition addNewColumn) {
    this.addNewColumn = addNewColumn;
  }

  public AlterTableOpType getAlterTableOpType() {
    return alterTableOpType;
  }

  public void setAlterTableOpType(AlterTableOpType alterTableOpType) {
    this.alterTableOpType = alterTableOpType;
  }

  public ColumnReferenceExpr[] getColumns() { return columns; }

  public void setColumns(ColumnReferenceExpr[] columns) { this.columns = columns; }

  public Expr[] getValues() { return values; }

  public void setValues(Expr[] values) { this.values = values; }

  public String getLocation() { return location; }

  public void setLocation(String location) { this.location = location; }

  public boolean hasParams() {
    return params != null;
  }

  public Map<String, String> getParams() {
    return params;
  }

  public void setParams(Map<String, String> params) {
    this.params = params;
  }

  public List<String> getUnsetPropertyKeys() {
    return unsetPropertyKeys;
  }

  public void setUnsetPropertyKeys(List<String> unsetPropertyKeys) {
    this.unsetPropertyKeys = unsetPropertyKeys;
  }

  public boolean isPurge() {
    return purge;
  }

  public void setPurge(boolean purge) {
    this.purge = purge;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

  public boolean isIfExists() {
    return ifExists;
  }

  public void setIfExists(boolean ifExists) {
    this.ifExists = ifExists;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tableName, newTableName, columnName, newColumnName, addNewColumn, alterTableOpType,
      columns, values, location, params, purge, ifNotExists, ifExists
    );

  }

  @Override
  boolean equalsTo(Expr expr) {
    AlterTable another = (AlterTable) expr;
    return tableName.equals(another.tableName) &&
        TUtil.checkEquals(newTableName, another.newTableName) &&
        TUtil.checkEquals(columnName, another.columnName) &&
        TUtil.checkEquals(newColumnName, another.newColumnName) &&
        TUtil.checkEquals(addNewColumn, another.addNewColumn) &&
        TUtil.checkEquals(alterTableOpType, another.alterTableOpType) &&
        TUtil.checkEquals(columns, another.columns) &&
        TUtil.checkEquals(values, another.values) &&
        TUtil.checkEquals(location, another.location) &&
        TUtil.checkEquals(params, another.params) &&
        TUtil.checkEquals(purge, another.purge) &&
        TUtil.checkEquals(ifNotExists, another.ifNotExists) &&
        TUtil.checkEquals(ifExists, another.ifExists)
    ;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    AlterTable alter = (AlterTable) super.clone();
    alter.tableName = tableName;
    alter.newTableName = newTableName;
    alter.columnName = columnName;
    alter.newColumnName = newColumnName;
    if (addNewColumn != null) {
      alter.addNewColumn = (ColumnDefinition) addNewColumn.clone();
    }
    alter.alterTableOpType = alterTableOpType;
    alter.columns = columns;
    alter.values = values;
    alter.location = location;
    if (params != null) {
      alter.params = new HashMap<>(params);
    }
    alter.purge = purge;
    alter.ifNotExists = ifNotExists;
    alter.ifExists = ifExists;
    return alter;
  }
}
