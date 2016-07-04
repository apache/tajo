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

package org.apache.tajo.plan.logical;


import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;

import org.apache.tajo.algebra.AlterTableOpType;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.plan.PlanString;
import org.apache.tajo.util.KeyValueSet;

public class AlterTableNode extends LogicalNode {

  @Expose
  private String tableName;
  @Expose
  private String newTableName;
  @Expose
  private String columnName;
  @Expose
  private String newColumnName;
  @Expose
  private Column addNewColumn;
  @Expose
  private KeyValueSet properties = new KeyValueSet();
  @Expose
  private String[] propertyKeys;
  @Expose
  private AlterTableOpType alterTableOpType;
  @Expose
  private String[] partitionColumns;
  @Expose
  private String[] partitionValues;
  @Expose
  private String location;
  @Expose
  private boolean isPurge;
  @Expose
  private boolean ifNotExists;
  @Expose
  private boolean ifExists;

  public AlterTableNode(int pid) {
    super(pid, NodeType.ALTER_TABLE);
  }

  @Override
  public int childNum() {
    return 0;
  }

  @Override
  public LogicalNode getChild(int idx) {
    return null;
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

  public Column getAddNewColumn() {
    return addNewColumn;
  }

  public void setAddNewColumn(Column addNewColumn) {
    this.addNewColumn = addNewColumn;
  }

  public AlterTableOpType getAlterTableOpType() {
    return alterTableOpType;
  }

  public void setAlterTableOpType(AlterTableOpType alterTableOpType) {
    this.alterTableOpType = alterTableOpType;
  }

  public boolean hasProperties() {
    return this.properties != null;
  }

  public KeyValueSet getProperties() {
    return this.properties;
  }

  public void setProperties(KeyValueSet properties) {
    this.properties = properties;
  }

  public String[] getPropertyKeys() {
    return propertyKeys;
  }

  public void setPropertyKeys(String[] propertyKeys) {
    this.propertyKeys = propertyKeys;
  }

  public String[] getPartitionColumns() {
    return partitionColumns;
  }

  public void setPartitionColumns(String[] partitionColumns) {
    this.partitionColumns = partitionColumns;
  }

  public String[] getPartitionValues() {
    return partitionValues;
  }

  public void setPartitionValues(String[] partitionValues) {
    this.partitionValues = partitionValues;
  }

  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public boolean isPurge() {
    return isPurge;
  }

  public void setPurge(boolean isPurge) {
    this.isPurge = isPurge;
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
  public PlanString getPlanString() {
    return new PlanString(this);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tableName, addNewColumn, alterTableOpType, columnName, newColumnName, newTableName,
      tableName, properties, propertyKeys, partitionColumns, partitionValues, location, isPurge, ifNotExists, ifExists);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof AlterTableNode) {
      AlterTableNode other = (AlterTableNode) obj;
      return super.equals(other);
    } else {
      return false;
    }
  }

    /*@Override
    public Object clone() throws CloneNotSupportedException {
        AlterTableNode alterTableNode = (AlterTableNode) super.clone();
        alterTableNode.tableName = tableName;
        alterTableNode.newTableName = newTableName;
        alterTableNode.columnName = columnName;
        alterTableNode.newColumnName=newColumnName;
        alterTableNode.addNewColumn =(Column) addNewColumn.clone();
        return alterTableNode;
    }*/

  @Override
  public String toString() {
    return "AlterTable (table=" + tableName + ")";
  }

  @Override
  public void preOrder(LogicalNodeVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public void postOrder(LogicalNodeVisitor visitor) {
    visitor.visit(this);
  }
}
