/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.plan.rewrite.rules;

import org.apache.tajo.exception.TajoException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.plan.logical.PartitionedTableScanNode;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRule;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRuleContext;
import org.apache.tajo.plan.serder.LogicalNodeDeserializer;
import org.apache.tajo.plan.serder.LogicalNodeSerializer;
import org.apache.tajo.plan.serder.PlanProto;
import org.apache.tajo.plan.util.PlannerUtil;

/**
 * It verifies the equality between the input and output of LogicalNodeTree(De)Serializer in logical planning.
 * It is used only for testing.
 */
@SuppressWarnings("unused")
public class LogicalPlanEqualityTester implements LogicalPlanRewriteRule {

  @Override
  public String getName() {
    return "LogicalPlanEqualityTester";
  }

  @Override
  public boolean isEligible(LogicalPlanRewriteRuleContext context) {
    return true;
  }

  @Override
  public LogicalPlan rewrite(LogicalPlanRewriteRuleContext context) throws TajoException {
    LogicalPlan plan = context.getPlan();
    LogicalNode root = plan.getRootBlock().getRoot();
    PlanProto.LogicalNodeTree serialized = LogicalNodeSerializer.serialize(plan.getRootBlock().getRoot());
    LogicalNode deserialized = LogicalNodeDeserializer.deserialize(context.getQueryContext(), null, serialized);

    // Error handling PartitionedTableScanNode because LogicalNodeDeserializer convert it to ScanNode.
    PartitionedTableScanNode partitionedTableScanNode = PlannerUtil.findTopNode(root, NodeType.PARTITIONS_SCAN);
    if (partitionedTableScanNode != null) {
      ScanNode scanNode = PlannerUtil.findTopNode(deserialized, NodeType.SCAN);
      assert scanNode != null;
    } else {
      assert root.deepEquals(deserialized);
    }
    return plan;
  }
}
