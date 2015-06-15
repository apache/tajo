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

package org.apache.tajo.storage.hbase;

import org.apache.tajo.OverridableConf;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.logical.SortNode.SortPurpose;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRule;
import org.apache.tajo.util.KeyValueSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This rewrite rule injects a sort operation to preserve the writing rows in
 * an ascending order of HBase row keys, required by HFile.
 */
public class SortedInsertRewriter implements LogicalPlanRewriteRule {

  @Override
  public String getName() {
    return "SortedInsertRewriter";
  }

  @Override
  public boolean isEligible(OverridableConf queryContext, LogicalPlan plan) {
    boolean hbaseMode = "false".equalsIgnoreCase(queryContext.get(HBaseStorageConstants.INSERT_PUT_MODE, "false"));
    LogicalRootNode rootNode = plan.getRootBlock().getRoot();
    LogicalNode node = rootNode.getChild();
    return hbaseMode && node.getType() == NodeType.CREATE_TABLE || node.getType() == NodeType.INSERT;
  }

  public static Column[] getIndexColumns(Schema tableSchema, KeyValueSet tableProperty) throws IOException {
    List<Column> indexColumns = new ArrayList<Column>();

    ColumnMapping columnMapping = new ColumnMapping(tableSchema, tableProperty);

    boolean[] isRowKeys = columnMapping.getIsRowKeyMappings();
    for (int i = 0; i < isRowKeys.length; i++) {
      if (isRowKeys[i]) {
        indexColumns.add(tableSchema.getColumn(i));
      }
    }

    return indexColumns.toArray(new Column[]{});
  }

  @Override
  public LogicalPlan rewrite(OverridableConf queryContext, LogicalPlan plan) throws PlanningException {
    LogicalRootNode rootNode = plan.getRootBlock().getRoot();

    StoreTableNode storeTable = rootNode.getChild();
    Schema tableSchema = storeTable.getTableSchema();

    Column[] sortColumns;
    try {
      sortColumns = getIndexColumns(tableSchema, storeTable.getOptions());
    } catch (IOException e) {
      throw new PlanningException(e);
    }

    int[] sortColumnIndexes = new int[sortColumns.length];
    for (int i = 0; i < sortColumns.length; i++) {
      sortColumnIndexes[i] = tableSchema.getColumnId(sortColumns[i].getQualifiedName());
    }

    UnaryNode insertNode = rootNode.getChild();
    LogicalNode childNode = insertNode.getChild();

    Schema sortSchema = childNode.getOutSchema();
    SortNode sortNode = plan.createNode(SortNode.class);
    sortNode.setSortPurpose(SortPurpose.STORAGE_SPECIFIED);
    sortNode.setInSchema(sortSchema);
    sortNode.setOutSchema(sortSchema);

    SortSpec[] sortSpecs = new SortSpec[sortColumns.length];
    int index = 0;

    for (int i = 0; i < sortColumnIndexes.length; i++) {
      Column sortColumn = sortSchema.getColumn(sortColumnIndexes[i]);
      if (sortColumn == null) {
        throw new PlanningException("Can't fine proper sort column:" + sortColumns[i]);
      }
      sortSpecs[index++] = new SortSpec(sortColumn, true, true);
    }
    sortNode.setSortSpecs(sortSpecs);

    sortNode.setChild(insertNode.getChild());
    insertNode.setChild(sortNode);
    plan.getRootBlock().registerNode(sortNode);

    return plan;
  }
}
