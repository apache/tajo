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

package org.apache.tajo.master.exec.prehook;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.logical.InsertNode;
import org.apache.tajo.plan.logical.NodeType;

public class InsertIntoHook implements DistributedQueryHook {

  @Override
  public boolean isEligible(QueryContext queryContext, LogicalPlan plan) {
    return plan.getRootBlock().getRootType() == NodeType.INSERT;
  }

  @Override
  public void hook(QueryContext queryContext, LogicalPlan plan) throws Exception {
    queryContext.setInsert();

    InsertNode insertNode = plan.getRootBlock().getNode(NodeType.INSERT);

    // Set QueryContext settings, such as output table name and output path.
    // It also remove data files if overwrite is true.
    if (insertNode.hasTargetTable()) { // INSERT INTO [TB_NAME]
      queryContext.setOutputTable(insertNode.getTableName());
      if (insertNode.hasPartition()) {
        queryContext.setPartitionMethod(insertNode.getPartitionMethod());
      }
    } else { // INSERT INTO LOCATION ...
      // When INSERT INTO LOCATION, must not set output table.
      queryContext.setFileOutput();
    }

    // Set the final output table uri
    queryContext.setOutputPath(insertNode.getUri());

    if (insertNode.isOverwrite()) {
      queryContext.setOutputOverwrite();
    }
  }
}
