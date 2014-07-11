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

  @Override
  public int hashCode() {
    return Objects.hashCode(tableName,
        null != newTableName ? Objects.hashCode(newTableName) : newTableName,
        null != columnName ? Objects.hashCode(columnName) : columnName,
        null != newColumnName ? Objects.hashCode(newColumnName) : newColumnName,
        null != addNewColumn ? Objects.hashCode(addNewColumn) : addNewColumn,
        null != alterTableOpType ? Objects.hashCode(alterTableOpType) : alterTableOpType);

  }

  @Override
  boolean equalsTo(Expr expr) {
    AlterTable another = (AlterTable) expr;
    return tableName.equals(another.tableName) &&
        TUtil.checkEquals(newTableName, another.newTableName) &&
        TUtil.checkEquals(columnName, another.columnName) &&
        TUtil.checkEquals(newColumnName, another.newColumnName) &&
        TUtil.checkEquals(addNewColumn, another.addNewColumn) &&
        TUtil.checkEquals(alterTableOpType, another.alterTableOpType);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    AlterTable alter = (AlterTable) super.clone();
    alter.tableName = tableName;
    alter.newTableName = newTableName;
    alter.columnName = columnName;
    alter.newColumnName = newColumnName;
    alter.addNewColumn = (ColumnDefinition) addNewColumn.clone();
    alter.alterTableOpType = alterTableOpType;
    return alter;
  }
}
