/*
 * Lisensed to the Apache Software Foundation (ASF) under one
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

package org.apache.tajo.engine.plan;

import com.google.common.collect.Maps;
import org.apache.tajo.engine.plan.proto.PlanProto;
import org.apache.tajo.engine.planner.BasicLogicalPlanVisitor;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.engine.planner.Target;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.NodeType;
import org.apache.tajo.engine.planner.logical.ScanNode;

import java.util.Map;
import java.util.Stack;

public class LogicalNodeTreeSerializer extends BasicLogicalPlanVisitor<LogicalNodeTreeSerializer.SerializeContext,
    LogicalNode> {

  private static final LogicalNodeTreeSerializer instance;

  static {
    instance = new LogicalNodeTreeSerializer();
  }

  public static PlanProto.LogicalNode serialize(LogicalNode node) throws PlanningException {
    instance.visit(new SerializeContext(), null, null, node, new Stack<LogicalNode>());

    return null;
  }

  public static PlanProto.NodeType convertType(NodeType type) {
    return PlanProto.NodeType.valueOf(type.name());
  }

  public static PlanProto.LogicalNode.Builder createNodeBuilder(SerializeContext context, LogicalNode node) {
    int selfId;
    if (context.idMap.containsKey(node)) {
      selfId = context.idMap.get(node);
    } else {
      selfId = context.seqId++;
      context.idMap.put(node, selfId);
    }

    PlanProto.LogicalNode.Builder nodeBuilder = PlanProto.LogicalNode.newBuilder();
    nodeBuilder.setSid(selfId);
    nodeBuilder.setPid(node.getPID());
    nodeBuilder.setType(convertType(node.getType()));
    nodeBuilder.setInSchema(node.getInSchema().getProto());
    nodeBuilder.setOutSchema(node.getOutSchema().getProto());
    return nodeBuilder;
  }

  public static class SerializeContext {
    private int seqId = 0;
    private Map<LogicalNode, Integer> idMap = Maps.newHashMap();
    private PlanProto.LogicalNodeTree.Builder treeBuilder = PlanProto.LogicalNodeTree.newBuilder();
  }

  @Override
  public LogicalNode visitScan(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                               ScanNode scan, Stack<LogicalNode> stack) throws PlanningException {

    // building itself
    PlanProto.ScanNode.Builder scanBuilder = PlanProto.ScanNode.newBuilder();
    scanBuilder.setTable(scan.getTableDesc().getProto());
    if (scan.hasAlias()) {
      scanBuilder.setAlias(scan.getAlias());
    }

    if (scan.hasTargets()) {
      for (Target t : scan.getTargets()) {
        PlanProto.Target.Builder targetBuilder = PlanProto.Target.newBuilder();

        targetBuilder.setExpr(EvalTreeProtoSerializer.serialize(t.getEvalTree()));

        if (t.hasAlias()) {
          targetBuilder.setAlias(t.getAlias());
        }
        scanBuilder.addTargets(targetBuilder);
      }
    }

    if (scan.hasQual()) {
      scanBuilder.setQual(EvalTreeProtoSerializer.serialize(scan.getQual()));
    }

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, scan);
    nodeBuilder.setScan(scanBuilder);

    context.treeBuilder.addNodes(nodeBuilder);

    return scan;
  }
}
