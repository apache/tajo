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
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.plan.proto.PlanProto;
import org.apache.tajo.engine.planner.BasicLogicalPlanVisitor;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.engine.planner.Target;
import org.apache.tajo.engine.planner.logical.*;

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
  public LogicalNode visitLimit(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                LimitNode limit, Stack<LogicalNode> stack) throws PlanningException {
    super.visitLimit(context, plan, block, limit, stack);

    int [] childIds = registerGetChildIds(context, limit);

    // building itself
    PlanProto.LimitNode.Builder limitBuilder = PlanProto.LimitNode.newBuilder();
    limitBuilder.setChildId(childIds[0]);
    limitBuilder.setFetchFirstNum(limit.getFetchFirstNum());

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, limit);
    nodeBuilder.setLimit(limitBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return limit;
  }

  @Override
  public LogicalNode visitSort(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                SortNode sort, Stack<LogicalNode> stack) throws PlanningException {
    super.visitSort(context, plan, block, sort, stack);

    int [] childIds = registerGetChildIds(context, sort);

    // building itself
    PlanProto.SortNode.Builder sortBuilder = PlanProto.SortNode.newBuilder();
    sortBuilder.setChildId(childIds[0]);
    for (int i = 0; i < sort.getSortKeys().length; i++) {
      sortBuilder.addSortSpecs(sort.getSortKeys()[i].getProto());
    }

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, sort);
    nodeBuilder.setSort(sortBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return sort;
  }

  public LogicalNode visitGroupBy(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                  GroupbyNode groupbyNode, Stack<LogicalNode> stack) throws PlanningException {
    super.visitGroupBy(context, plan, block, groupbyNode, new Stack<LogicalNode>());

    int [] childIds = registerGetChildIds(context, groupbyNode);

    PlanProto.GroupbyNode.Builder groupbyBuilder = PlanProto.GroupbyNode.newBuilder();
    groupbyBuilder.setChildId(childIds[0]);

    for (int i = 0; i < groupbyNode.getGroupingColumns().length; i++) {
      groupbyBuilder.addGroupingKeys(groupbyNode.getGroupingColumns()[i].getProto());
    }
    for (int i = 0; i < groupbyNode.getGroupingColumns().length; i++) {
      groupbyBuilder.addAggFunctions(EvalTreeProtoSerializer.serialize(groupbyNode.getAggFunctions()[i]));
    }

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, groupbyNode);
    nodeBuilder.setGroupby(groupbyBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return groupbyNode;
  }

  @Override
  public LogicalNode visitFilter(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 SelectionNode filter, Stack<LogicalNode> stack) throws PlanningException {
    super.visitFilter(context, plan, block, filter, stack);

    int [] childIds = registerGetChildIds(context, filter);

    // building itself
    PlanProto.FilterNode.Builder filterBuilder = PlanProto.FilterNode.newBuilder();
    filterBuilder.setChildId(childIds[0]);
    filterBuilder.setQual(EvalTreeProtoSerializer.serialize(filter.getQual()));

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, filter);
    nodeBuilder.setFilter(filterBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return filter;
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

  public static PlanProto.NodeType convertType(NodeType type) {
    return PlanProto.NodeType.valueOf(type.name());
  }

  private int [] registerGetChildIds(SerializeContext context, LogicalNode node) {
    int [] childIds = new int[node.childNum()];
    for (int i = 0; i < node.childNum(); i++) {
      if (context.idMap.containsKey(node.getChild(i))) {
        childIds[i] = context.idMap.get(node.getChild(i));
      } else {
        childIds[i] = context.seqId++;
      }
    }
    return childIds;
  }
}
