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
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.LogicalRootNode;
import org.apache.tajo.plan.logical.SortNode;
import org.apache.tajo.plan.logical.SortNode.SortPurpose;
import org.apache.tajo.plan.logical.UnaryNode;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRule;
import org.apache.tajo.plan.util.PlannerUtil;

public class AddSortForInsertRewriter implements LogicalPlanRewriteRule {
  private int[] sortColumnIndexes;
  private Column[] sortColumns;

  public AddSortForInsertRewriter(TableDesc tableDesc, Column[] sortColumns) {
    this.sortColumns = sortColumns;
    this.sortColumnIndexes = new int[sortColumns.length];

    Schema tableSchema = tableDesc.getSchema();
    for (int i = 0; i < sortColumns.length; i++) {
      sortColumnIndexes[i] = tableSchema.getColumnId(sortColumns[i].getQualifiedName());
    }
  }

  @Override
  public String getName() {
    return "AddSortForInsertRewriter";
  }

  @Override
  public boolean isEligible(OverridableConf queryContext, LogicalPlan plan) {
    String storeType = PlannerUtil.getStoreType(plan);
    return storeType != null;
  }

  @Override
  public LogicalPlan rewrite(OverridableConf queryContext, LogicalPlan plan) throws PlanningException {
    LogicalRootNode rootNode = plan.getRootBlock().getRoot();
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
