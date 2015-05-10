///**
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.tajo.engine.planner.global.rewriter.rules;
//
//import org.apache.tajo.ExecutionBlockId;
//import org.apache.tajo.OverridableConf;
//import org.apache.tajo.engine.planner.global.ExecutionBlock;
//import org.apache.tajo.engine.planner.global.MasterPlan;
//import org.apache.tajo.engine.planner.global.rewriter.GlobalPlanRewriteRule;
//import org.apache.tajo.plan.LogicalPlan;
//import org.apache.tajo.plan.PlanningException;
//import org.apache.tajo.plan.logical.LogicalNode;
//import org.apache.tajo.plan.logical.NodeType;
//import org.apache.tajo.plan.logical.UnionNode;
//import org.apache.tajo.plan.util.PlannerUtil;
//import org.apache.tajo.util.TUtil;
//import org.apache.tajo.util.graph.DirectedGraphVisitor;
//
//import java.util.List;
//import java.util.Map;
//import java.util.Stack;
//
//public class UnionReduceRule implements GlobalPlanRewriteRule {
//
//  @Override
//  public String getName() {
//    return "UnionReduceRule";
//  }
//
//  @Override
//  public boolean isEligible(OverridableConf queryContext, MasterPlan plan) {
//    for (LogicalPlan.QueryBlock block : plan.getLogicalPlan().getQueryBlocks()) {
//      if (block.hasNode(NodeType.UNION)) {
//        return true;
//      }
//    }
//    return false;
//  }
//
//  @Override
//  public MasterPlan rewrite(MasterPlan plan) throws PlanningException {
//    Rewriter.rewrite(plan);
//    return plan;
//  }
//
//  static class Rewriter implements DirectedGraphVisitor<ExecutionBlockId> {
//    private static Rewriter instance;
//    private static MasterPlan plan;
//
//    private Rewriter() {}
//
//    public static void rewrite(MasterPlan plan) {
//      if (instance == null) {
//        instance = new Rewriter();
//      }
//      instance.plan = plan;
//      instance.visit(new Stack<ExecutionBlockId>(), plan.getTerminalBlock().getId());
//    }
//
//    @Override
//    public void visit(Stack<ExecutionBlockId> stack, ExecutionBlockId executionBlockId) {
//      // must have the form of
//      /*
//            parent_op
//               |
//             union
//             /   \
//      child_op  child_op
//       */
//      ExecutionBlock current = instance.plan.getExecBlock(executionBlockId);
//      if (current.hasUnion()) {
//        Map<Integer, ExecutionBlock> newBlocks = TUtil.newHashMap();
//        List<ExecutionBlock> childBlocks = instance.plan.getChilds(current);
//        // create new execution blocks for each child_op
//        // TODO: consider union sequence
//        UnionNode unionNode = PlannerUtil.findTopNode(current.getPlan(), NodeType.UNION);
//        LogicalNode parentOfUnion = PlannerUtil.findTopParentNode(current.getPlan(), NodeType.UNION);
//        newBlocks.put(unionNode.getLeftChild().getPID(), instance.plan.newExecutionBlock());
//        newBlocks.put(unionNode.getRightChild().getPID(), instance.plan.newExecutionBlock());
//
//        // push parent_op as the parent of each child_op and remove union
//
//
//        // connect new execution blocks and parents of the current block
//
//      }
//    }
//  }
//}
