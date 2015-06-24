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

package org.apache.tajo.engine.planner.global.rewriter.rules;

import org.apache.tajo.OverridableConf;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.ExecutionBlockCursor;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.global.rewriter.GlobalPlanRewriteRule;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.serder.LogicalNodeDeserializer;
import org.apache.tajo.plan.serder.LogicalNodeSerializer;
import org.apache.tajo.plan.serder.PlanProto;

/**
 * It verifies the equality between the input and output of LogicalNodeTree(De)Serializer in global planning.
 */
public class GlobalPlanEqualityTester implements GlobalPlanRewriteRule {

  @Override
  public String getName() {
    return "GlobalPlanEqualityTester";
  }

  @Override
  public boolean isEligible(OverridableConf queryContext, MasterPlan plan) {
    return true;
  }

  @Override
  public MasterPlan rewrite(MasterPlan plan) {
    try {
      ExecutionBlockCursor cursor = new ExecutionBlockCursor(plan);
      for (ExecutionBlock eb : cursor) {
        LogicalNode node = eb.getPlan();
        if (node != null) {
          PlanProto.LogicalNodeTree tree = LogicalNodeSerializer.serialize(node);
          LogicalNode deserialize = LogicalNodeDeserializer.deserialize(plan.getContext(), null, tree);
          assert node.deepEquals(deserialize);
        }
      }
      return plan;
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }
}
