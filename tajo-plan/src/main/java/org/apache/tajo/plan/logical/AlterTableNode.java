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
  private AlterTableOpType alterTableOpType;

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

  @Override
  public PlanString getPlanString() {
    return new PlanString(this);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((addNewColumn == null) ? 0 : addNewColumn.hashCode());
    result = prime * result + ((alterTableOpType == null) ? 0 : alterTableOpType.hashCode());
    result = prime * result + ((columnName == null) ? 0 : columnName.hashCode());
    result = prime * result + ((newColumnName == null) ? 0 : newColumnName.hashCode());
    result = prime * result + ((newTableName == null) ? 0 : newTableName.hashCode());
    result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
    result = prime * result + ((properties == null) ? 0 : properties.hashCode());
    return result;
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
