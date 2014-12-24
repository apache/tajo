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

package org.apache.tajo.plan.serder;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;

import java.util.List;
import java.util.Map;
import java.util.Stack;

public class LogicalNodeTreeSerializer extends BasicLogicalPlanVisitor<LogicalNodeTreeSerializer.SerializeContext,
    LogicalNode> {

  private static final LogicalNodeTreeSerializer instance;

  static {
    instance = new LogicalNodeTreeSerializer();
  }

  public static PlanProto.LogicalNodeTree serialize(LogicalNode node) throws PlanningException {
    SerializeContext context = new SerializeContext();
    instance.visit(context, null, null, node, new Stack<LogicalNode>());
    return context.treeBuilder.build();
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

  public LogicalNode visitRoot(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                               LogicalRootNode root, Stack<LogicalNode> stack) throws PlanningException {
    super.visitRoot(context, plan, block, root, stack);

    int [] childIds = registerGetChildIds(context, root);

    PlanProto.RootNode.Builder rootBuilder = PlanProto.RootNode.newBuilder();
    rootBuilder.setChildId(childIds[0]);

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, root);
    nodeBuilder.setRoot(rootBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return root;
  }

  public LogicalNode visitEvalExpr(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                   EvalExprNode exprEval, Stack<LogicalNode> stack) throws PlanningException {
    PlanProto.EvalExprNode.Builder exprEvalBuilder = PlanProto.EvalExprNode.newBuilder();
    exprEvalBuilder.addAllTargets(
        LogicalNodeTreeSerializer.<PlanProto.Target>toProtoObjects(exprEval.getTargets()));

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, exprEval);
    nodeBuilder.setExprEval(exprEvalBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return exprEval;
  }

  public LogicalNode visitProjection(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                     ProjectionNode projection, Stack<LogicalNode> stack) throws PlanningException {
    super.visitProjection(context, plan, block, projection, stack);

    int [] childIds = registerGetChildIds(context, projection);

    PlanProto.ProjectionNode.Builder projectionBuilder = PlanProto.ProjectionNode.newBuilder();
    projectionBuilder.setChildId(childIds[0]);
    projectionBuilder.addAllTargets(
        LogicalNodeTreeSerializer.<PlanProto.Target>toProtoObjects(projection.getTargets()));

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, projection);
    nodeBuilder.setProjection(projectionBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return projection;
  }

  @Override
  public LogicalNode visitLimit(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                LimitNode limit, Stack<LogicalNode> stack) throws PlanningException {
    super.visitLimit(context, plan, block, limit, stack);

    int [] childIds = registerGetChildIds(context, limit);

    PlanProto.LimitNode.Builder limitBuilder = PlanProto.LimitNode.newBuilder();
    limitBuilder.setChildId(childIds[0]);
    limitBuilder.setFetchFirstNum(limit.getFetchFirstNum());

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, limit);
    nodeBuilder.setLimit(limitBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return limit;
  }

  public LogicalNode visitWindowAgg(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                    WindowAggNode windowAgg, Stack<LogicalNode> stack) throws PlanningException {
    super.visitWindowAgg(context, plan, block, windowAgg, stack);

    int [] childIds = registerGetChildIds(context, windowAgg);

    PlanProto.WindowAggNode.Builder windowAggBuilder = PlanProto.WindowAggNode.newBuilder();
    windowAggBuilder.setChildId(childIds[0]);
    if (windowAgg.hasPartitionKeys()) {
      windowAggBuilder.addAllPartitionKeys(
          LogicalNodeTreeSerializer.<CatalogProtos.ColumnProto>toProtoObjects(windowAgg.getPartitionKeys()));
    }
    if (windowAgg.hasAggFunctions()) {
      windowAggBuilder.addAllWindowFunctions(
          LogicalNodeTreeSerializer.<PlanProto.EvalTree>toProtoObjects(windowAgg.getWindowFunctions()));
    }
    windowAggBuilder.setDistinct(windowAgg.isDistinct());

    if (windowAgg.hasSortSpecs()) {
      windowAggBuilder.addAllSortSpecs(
          LogicalNodeTreeSerializer.<CatalogProtos.SortSpecProto>toProtoObjects(windowAgg.getSortSpecs()));
    }
    if (windowAgg.hasTargets()) {
      windowAggBuilder.addAllTargets(
          LogicalNodeTreeSerializer.<PlanProto.Target>toProtoObjects(windowAgg.getTargets()));
    }

    return windowAgg;
  }

  @Override
  public LogicalNode visitSort(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                SortNode sort, Stack<LogicalNode> stack) throws PlanningException {
    super.visitSort(context, plan, block, sort, stack);

    int [] childIds = registerGetChildIds(context, sort);

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

  @Override
  public LogicalNode visitHaving(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 HavingNode having, Stack<LogicalNode> stack) throws PlanningException {
    super.visitHaving(context, plan, block, having, stack);

    int [] childIds = registerGetChildIds(context, having);

    PlanProto.FilterNode.Builder filterBuilder = PlanProto.FilterNode.newBuilder();
    filterBuilder.setChildId(childIds[0]);
    filterBuilder.setQual(EvalTreeProtoSerializer.serialize(having.getQual()));

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, having);
    nodeBuilder.setFilter(filterBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return having;
  }

  public LogicalNode visitGroupBy(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                  GroupbyNode groupbyNode, Stack<LogicalNode> stack) throws PlanningException {
    super.visitGroupBy(context, plan, block, groupbyNode, new Stack<LogicalNode>());

    int [] childIds = registerGetChildIds(context, groupbyNode);

    PlanProto.GroupbyNode.Builder groupbyBuilder = PlanProto.GroupbyNode.newBuilder();
    groupbyBuilder.setChildId(childIds[0]);

    if (groupbyNode.groupingKeyNum() > 0) {
      groupbyBuilder.addAllGroupingKeys(
          LogicalNodeTreeSerializer.<CatalogProtos.ColumnProto>toProtoObjects(groupbyNode.getGroupingColumns()));
    }
    if (groupbyNode.hasAggFunctions()) {
      groupbyBuilder.addAllAggFunctions(
          LogicalNodeTreeSerializer.<PlanProto.EvalTree>toProtoObjects(groupbyNode.getAggFunctions()));
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

    PlanProto.FilterNode.Builder filterBuilder = PlanProto.FilterNode.newBuilder();
    filterBuilder.setChildId(childIds[0]);
    filterBuilder.setQual(EvalTreeProtoSerializer.serialize(filter.getQual()));

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, filter);
    nodeBuilder.setFilter(filterBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return filter;
  }

  public LogicalNode visitJoin(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, JoinNode join,
                          Stack<LogicalNode> stack) throws PlanningException {
    super.visitJoin(context, plan, block, join, stack);

    int [] childIds = registerGetChildIds(context, join);

    // building itself
    PlanProto.JoinNode.Builder joinBuilder = PlanProto.JoinNode.newBuilder();
    joinBuilder.setJoinType(convertJoinType(join.getJoinType()));
    joinBuilder.setLeftChildId(childIds[0]);
    joinBuilder.setRightChildId(childIds[1]);
    if (join.hasJoinQual()) {
      joinBuilder.setJoinQual(EvalTreeProtoSerializer.serialize(join.getJoinQual()));
    }
    if (join.hasTargets()) {
      joinBuilder.addAllTargets(LogicalNodeTreeSerializer.<PlanProto.Target>toProtoObjects(join.getTargets()));
    }

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, join);
    nodeBuilder.setJoin(joinBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return join;
  }

  @Override
  public LogicalNode visitScan(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                               ScanNode scan, Stack<LogicalNode> stack) throws PlanningException {

    PlanProto.ScanNode.Builder scanBuilder = PlanProto.ScanNode.newBuilder();
    scanBuilder.setTable(scan.getTableDesc().getProto());
    if (scan.hasAlias()) {
      scanBuilder.setAlias(scan.getAlias());
    }

    if (scan.hasTargets()) {
      scanBuilder.addAllTargets(LogicalNodeTreeSerializer.<PlanProto.Target>toProtoObjects(scan.getTargets()));
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

  public static PlanProto.JoinType convertJoinType(JoinType type) {
    switch (type) {
    case CROSS:
      return PlanProto.JoinType.CROSS_JOIN;
    case INNER:
      return PlanProto.JoinType.INNER_JOIN;
    case LEFT_OUTER:
      return PlanProto.JoinType.LEFT_OUTER_JOIN;
    case RIGHT_OUTER:
      return PlanProto.JoinType.RIGHT_OUTER_JOIN;
    case FULL_OUTER:
      return PlanProto.JoinType.FULL_OUTER_JOIN;
    case LEFT_SEMI:
      return PlanProto.JoinType.LEFT_SEMI_JOIN;
    case RIGHT_SEMI:
      return PlanProto.JoinType.RIGHT_SEMI_JOIN;
    case LEFT_ANTI:
      return PlanProto.JoinType.LEFT_ANTI_JOIN;
    case RIGHT_ANTI:
      return PlanProto.JoinType.RIGHT_ANTI_JOIN;
    case UNION:
      return PlanProto.JoinType.UNION_JOIN;
    default:
      throw new RuntimeException("Unknown JoinType: " + type.name());
    }
  }

  public static PlanProto.Target convertTarget(Target target) {
    PlanProto.Target.Builder targetBuilder = PlanProto.Target.newBuilder();
    targetBuilder.setExpr(EvalTreeProtoSerializer.serialize(target.getEvalTree()));
    if (target.hasAlias()) {
      targetBuilder.setAlias(target.getAlias());
    }
    return targetBuilder.build();
  }

  public static <T> Iterable<T> toProtoObjects(ProtoObject[] protoObjects) {
    List<T> converted = Lists.newArrayList();
    for (int i = 0; i < protoObjects.length; i++) {
      converted.add((T) protoObjects[i].getProto());
    }
    return converted;
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
