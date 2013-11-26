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

package org.apache.tajo.engine.planner.global;

import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.engine.planner.logical.GroupbyNode;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.NodeType;
import org.apache.tajo.engine.planner.logical.SortNode;

public class DestinationContext {
  @Expose private String destTableName;
  @Expose private StoreType storeType = StoreType.CSV;
  @Expose private NodeType terminalNodeType;
  @Expose private Schema outputSchema;
  @Expose private Column[] groupingColumns;
  @Expose private SortSpec[] sortSpecs;

  public DestinationContext() {

  }

  public DestinationContext(LogicalNode node) {
    this.set(node);
  }

  public void set(LogicalNode node) {
    terminalNodeType = node.getType();
    outputSchema = node.getOutSchema();
    if (terminalNodeType.equals(NodeType.GROUP_BY)) {
      groupingColumns = ((GroupbyNode)node).getGroupingColumns();
    } else if (terminalNodeType.equals(NodeType.SORT)) {
      sortSpecs = ((SortNode)node).getSortKeys();
    }
  }

  public NodeType getTerminalNodeType() {
    return terminalNodeType;
  }

  public void setTerminalNodeType(NodeType terminalNodeType) {
    this.terminalNodeType = terminalNodeType;
  }

  public Schema getOutputSchema() {
    return outputSchema;
  }

  public void setOutputSchema(Schema outputSchema) {
    this.outputSchema = outputSchema;
  }

  public Column [] getGroupingColumns() {
    return groupingColumns;
  }

  public void setGroupingColumns(Column [] groupingColumns) {
    this.groupingColumns = groupingColumns;
  }

  public SortSpec[] getSortSpecs() {
    return sortSpecs;
  }

  public void setSortSpecs(SortSpec[] sortSpecs) {
    this.sortSpecs = sortSpecs;
  }

  public StoreType getStoreType() {
    return storeType;
  }

  public void setStoreType(StoreType storeType) {
    this.storeType = storeType;
  }

  public String getDestTableName() {
    return destTableName;
  }

  public void setDestTableName(String destTableName) {
    this.destTableName = destTableName;
  }
}
