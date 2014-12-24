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
import org.apache.tajo.OverridableConf;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.AggregationFunctionCallEval;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.*;

import java.util.*;

public class LogicalNodeTreeDeserializer {
  private static final LogicalNodeTreeDeserializer instance;

  static {
    instance = new LogicalNodeTreeDeserializer();
  }

  public static LogicalNode deserialize(OverridableConf context, PlanProto.LogicalNodeTree tree) {
    Map<Integer, LogicalNode> nodeMap = Maps.newHashMap();

    // sort serialized logical nodes in an ascending order of their sids
    List<PlanProto.LogicalNode> nodeList = Lists.newArrayList(tree.getNodesList());
    Collections.sort(nodeList, new Comparator<PlanProto.LogicalNode>() {
      @Override
      public int compare(PlanProto.LogicalNode o1, PlanProto.LogicalNode o2) {
        return o1.getSid() - o2.getSid();
      }
    });

    LogicalNode current = null;

    // The sorted order is the same of a postfix traverse order.
    // So, it sequentially transforms each serialized node into a LogicalNode instance in a postfix order of
    // the original logical node tree.

    Iterator<PlanProto.LogicalNode> it = nodeList.iterator();
    while (it.hasNext()) {
      PlanProto.LogicalNode protoNode = it.next();

      switch (protoNode.getType()) {
      case ROOT:
        current = convertRoot(nodeMap, protoNode);
        break;
      case EXPRS:
        current = convertEvalExpr(context, protoNode);
        break;
      case PROJECTION:
        current = convertProjection(context, nodeMap, protoNode);
        break;
      case LIMIT:
        current = convertLimit(nodeMap, protoNode);
        break;
      case SORT:
        current = convertSort(nodeMap, protoNode);
        break;
      case HAVING:
        current = convertHaving(context, nodeMap, protoNode);
        break;
      case GROUP_BY:
        current = convertGroupby(context, nodeMap, protoNode);
        break;
      case SELECTION:
        current = convertFilter(context, nodeMap, protoNode);
        break;
      case JOIN:
        current = convertJoin(context, nodeMap, protoNode);
        break;
      case SCAN:
        current = convertScan(context, protoNode);
        break;

      default:
        throw new RuntimeException("Unknown NodeType: " + protoNode.getType().name());
      }

      nodeMap.put(protoNode.getPid(), current);
    }

    return current;
  }

  public static LogicalRootNode convertRoot(Map<Integer, LogicalNode> nodeMap,
                                            PlanProto.LogicalNode protoNode) {
    PlanProto.RootNode rootProto = protoNode.getRoot();

    LogicalRootNode root = new LogicalRootNode(protoNode.getPid());
    root.setChild(nodeMap.get(rootProto.getChildId()));

    return root;
  }

  public static EvalExprNode convertEvalExpr(OverridableConf context, PlanProto.LogicalNode protoNode) {
    PlanProto.EvalExprNode evalExprProto = protoNode.getExprEval();

    EvalExprNode evalExpr = new EvalExprNode(protoNode.getPid());
    evalExpr.setInSchema(convertSchema(protoNode.getInSchema()));
    evalExpr.setTargets(convertTargets(context, evalExprProto.getTargetsList()));

    return evalExpr;
  }

  public static ProjectionNode convertProjection(OverridableConf context, Map<Integer, LogicalNode> nodeMap,
                                                 PlanProto.LogicalNode protoNode) {
    PlanProto.ProjectionNode projectionProto = protoNode.getProjection();

    ProjectionNode projectionNode = new ProjectionNode(protoNode.getPid());
    projectionNode.init(projectionProto.getDistinct(), convertTargets(context, projectionProto.getTargetsList()));
    projectionNode.setChild(nodeMap.get(projectionProto.getChildId()));
    projectionNode.setInSchema(convertSchema(protoNode.getInSchema()));
    projectionNode.setTargets(convertTargets(context, projectionProto.getTargetsList()));

    return projectionNode;
  }

  public static LimitNode convertLimit(Map<Integer, LogicalNode> nodeMap, PlanProto.LogicalNode protoNode) {
    PlanProto.LimitNode limitProto = protoNode.getLimit();

    LimitNode limitNode = new LimitNode(protoNode.getPid());
    limitNode.setChild(nodeMap.get(limitProto.getChildId()));
    limitNode.setInSchema(convertSchema(protoNode.getInSchema()));
    limitNode.setOutSchema(convertSchema(protoNode.getOutSchema()));
    limitNode.setFetchFirst(limitProto.getFetchFirstNum());

    return limitNode;
  }

  public static SortNode convertSort(Map<Integer, LogicalNode> nodeMap, PlanProto.LogicalNode protoNode) {
    PlanProto.SortNode sortProto = protoNode.getSort();

    SortNode sortNode = new SortNode(protoNode.getPid());
    sortNode.setChild(nodeMap.get(sortProto.getChildId()));
    sortNode.setInSchema(convertSchema(protoNode.getInSchema()));
    sortNode.setOutSchema(convertSchema(protoNode.getOutSchema()));
    sortNode.setSortSpecs(convertSortSpecs(sortProto.getSortSpecsList()));

    return sortNode;
  }

  public static HavingNode convertHaving(OverridableConf context, Map<Integer, LogicalNode> nodeMap,
                                         PlanProto.LogicalNode protoNode) {
    PlanProto.FilterNode havingProto = protoNode.getFilter();

    HavingNode having = new HavingNode(protoNode.getPid());
    having.setChild(nodeMap.get(havingProto.getChildId()));
    having.setQual(EvalTreeProtoDeserializer.deserialize(context, havingProto.getQual()));
    having.setInSchema(convertSchema(protoNode.getInSchema()));
    having.setOutSchema(convertSchema(protoNode.getOutSchema()));

    return having;
  }

  public static GroupbyNode convertGroupby(OverridableConf context, Map<Integer, LogicalNode> nodeMap,
                                           PlanProto.LogicalNode protoNode) {
    PlanProto.GroupbyNode groupbyProto = protoNode.getGroupby();

    GroupbyNode groupby = new GroupbyNode(protoNode.getPid());
    groupby.setChild(nodeMap.get(groupbyProto.getChildId()));
    groupby.setInSchema(convertSchema(protoNode.getInSchema()));
    groupby.setOutSchema(convertSchema(protoNode.getOutSchema()));
    if (groupbyProto.getAggFunctionsCount() > 0) {
      groupby.setGroupingColumns(convertColumns(groupbyProto.getGroupingKeysList()));
    }
    if (groupbyProto.getAggFunctionsCount() > 0) {
      groupby.setAggFunctions((AggregationFunctionCallEval[])convertEvalNodes(context,
          groupbyProto.getAggFunctionsList()));
    }
    return groupby;
  }

  public static JoinNode convertJoin(OverridableConf context, Map<Integer, LogicalNode> nodeMap,
                                     PlanProto.LogicalNode protoNode) {
    PlanProto.JoinNode joinProto = protoNode.getJoin();

    JoinNode join = new JoinNode(protoNode.getPid());
    join.setLeftChild(nodeMap.get(joinProto.getLeftChildId()));
    join.setRightChild(nodeMap.get(joinProto.getRightChildId()));
    join.setJoinType(convertJoinType(joinProto.getJoinType()));
    join.setInSchema(convertSchema(protoNode.getInSchema()));
    join.setOutSchema(convertSchema(protoNode.getOutSchema()));
    if (joinProto.hasJoinQual()) {
      join.setJoinQual(EvalTreeProtoDeserializer.deserialize(context, joinProto.getJoinQual()));
    }
    if (joinProto.getTargetsCount() > 0) {
      join.setTargets(convertTargets(context, joinProto.getTargetsList()));
    }

    return join;
  }

  public static SelectionNode convertFilter(OverridableConf context, Map<Integer, LogicalNode> nodeMap,
                                            PlanProto.LogicalNode protoNode) {
    PlanProto.FilterNode filterProto = protoNode.getFilter();

    SelectionNode selection = new SelectionNode(protoNode.getPid());
    selection.setInSchema(convertSchema(protoNode.getInSchema()));
    selection.setOutSchema(convertSchema(protoNode.getOutSchema()));
    selection.setChild(nodeMap.get(filterProto.getChildId()));
    selection.setQual(EvalTreeProtoDeserializer.deserialize(context, filterProto.getQual()));

    return selection;
  }

  public static ScanNode convertScan(OverridableConf context, PlanProto.LogicalNode protoNode) {
    ScanNode scan = new ScanNode(protoNode.getPid());

    PlanProto.ScanNode scanProto = protoNode.getScan();
    if (scanProto.hasAlias()) {
      scan.init(new TableDesc(scanProto.getTable()), scanProto.getAlias());
    } else {
      scan.init(new TableDesc(scanProto.getTable()));
    }

    if (scanProto.getTargetsCount() > 0) {
      scan.setTargets(convertTargets(context, scanProto.getTargetsList()));
    }

    return scan;
  }

  public static Schema convertSchema(CatalogProtos.SchemaProto proto) {
    return new Schema(proto);
  }

  public static <T extends EvalNode> T [] convertEvalNodes(OverridableConf context,
                                                           List<PlanProto.EvalTree> evalTrees) {
    EvalNode [] evalNodes = new EvalNode[evalTrees.size()];
    for (int i = 0; i < evalNodes.length; i++) {
      evalNodes[i] = EvalTreeProtoDeserializer.deserialize(context, evalTrees.get(i));
    }
    return (T[]) evalNodes;
  }

  public static Column[] convertColumns(List<CatalogProtos.ColumnProto> columnProtos) {
    Column [] columns = new Column[columnProtos.size()];
    for (int i = 0; i < columns.length; i++) {
      columns[i] = new Column(columnProtos.get(i));
    }
    return columns;
  }

  public static Target[] convertTargets(OverridableConf context, List<PlanProto.Target> targetsProto) {
    Target [] targets = new Target[targetsProto.size()];
    for (int i = 0; i < targets.length; i++) {
      PlanProto.Target targetProto = targetsProto.get(i);
      targets[i] = new Target(EvalTreeProtoDeserializer.deserialize(context, targetProto.getExpr()),
          targetProto.getAlias());
    }
    return targets;
  }

  public static SortSpec[] convertSortSpecs(List<CatalogProtos.SortSpecProto> sortSpecProtos) {
    SortSpec[] sortSpecs = new SortSpec[sortSpecProtos.size()];
    int i = 0;
    for (CatalogProtos.SortSpecProto proto : sortSpecProtos) {
      sortSpecs[i++] = new SortSpec(proto);
    }
    return sortSpecs;
  }

  public static JoinType convertJoinType(PlanProto.JoinType type) {
    switch (type) {
    case CROSS_JOIN:
      return JoinType.CROSS;
    case INNER_JOIN:
      return JoinType.INNER;
    case LEFT_OUTER_JOIN:
      return JoinType.LEFT_OUTER;
    case RIGHT_OUTER_JOIN:
      return JoinType.RIGHT_OUTER;
    case FULL_OUTER_JOIN:
      return JoinType.FULL_OUTER;
    case LEFT_SEMI_JOIN:
      return JoinType.LEFT_SEMI;
    case RIGHT_SEMI_JOIN:
      return JoinType.RIGHT_SEMI;
    case LEFT_ANTI_JOIN:
      return JoinType.LEFT_ANTI;
    case RIGHT_ANTI_JOIN:
      return JoinType.RIGHT_ANTI;
    case UNION_JOIN:
      return JoinType.UNION;
    default:
      throw new RuntimeException("Unknown JoinType: " + type.name());
    }
  }
}
