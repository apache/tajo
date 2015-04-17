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

import org.apache.tajo.OverridableConf;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRule;
import org.apache.tajo.plan.serder.LogicalNodeDeserializer;
import org.apache.tajo.plan.serder.LogicalNodeSerializer;
import org.apache.tajo.plan.serder.PlanProto;

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
  public boolean isEligible(OverridableConf queryContext, LogicalPlan plan) {
    return true;
  }

  @Override
  public LogicalPlan rewrite(OverridableConf queryContext, LogicalPlan plan) throws PlanningException {
    LogicalNode root = plan.getRootBlock().getRoot();
    PlanProto.LogicalNodeTree serialized = LogicalNodeSerializer.serialize(plan.getRootBlock().getRoot());
    LogicalNode deserialized = LogicalNodeDeserializer.deserialize(queryContext, null, serialized);
    assert root.deepEquals(deserialized);
    return plan;
  }
}
