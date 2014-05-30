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

package org.apache.tajo.engine.planner.rewrite;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.exception.InvalidQueryException;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.planner.rewrite.FilterPushDownRule.FilterPushDownContext;
import org.apache.tajo.util.TUtil;

import java.util.*;

/**
 * This rule tries to push down all filter conditions into logical nodes as lower as possible.
 * It is likely to significantly reduces the intermediate data.
 */
public class FilterPushDownRule extends BasicLogicalPlanVisitor<FilterPushDownContext, LogicalNode>
    implements RewriteRule {
  private final static Log LOG = LogFactory.getLog(FilterPushDownRule.class);
  private static final String NAME = "FilterPushDown";

  static class FilterPushDownContext {
    Set<EvalNode> pushingDownFilters = new HashSet<EvalNode>();

    public void clear() {
      pushingDownFilters.clear();
    }
    public void setFiltersTobePushed(Collection<EvalNode> workingEvals) {
      this.pushingDownFilters.clear();
      this.pushingDownFilters.addAll(workingEvals);
    }
    public void addFiltersTobePushed(Collection<EvalNode> workingEvals) {
      this.pushingDownFilters.addAll(workingEvals);
    }

    public void setToOrigin(Map<EvalNode, EvalNode> evalMap) {
      //evalMap: copy -> origin
      List<EvalNode> origins = new ArrayList<EvalNode>();
      for (EvalNode eval : pushingDownFilters) {
        EvalNode origin = evalMap.get(eval);
        if (origin != null) {
          origins.add(origin);
        }
      }
      setFiltersTobePushed(origins);
    }
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean isEligible(LogicalPlan plan) {
    for (LogicalPlan.QueryBlock block : plan.getQueryBlocks()) {
      if (block.hasNode(NodeType.SELECTION) || block.hasNode(NodeType.JOIN)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public LogicalPlan rewrite(LogicalPlan plan) throws PlanningException {
    /*
    FilterPushDown rule: processing when visits each node
      - If a target which is corresponding on a filter EvalNode's column is not FieldEval, do not PushDown.
      - Replace filter EvalNode's column with child node's output column.
        If there is no child node's output column, do not PushDown.
      - When visit ScanNode, add filter eval to ScanNode's qual
      - When visit GroupByNode, Find aggregation column in a filter EvalNode and
        . If a parent is HavingNode, add filter eval to parent HavingNode.
        . It not, create new HavingNode and set parent's child.
     */
    FilterPushDownContext context = new FilterPushDownContext();
    for (LogicalPlan.QueryBlock block : plan.getQueryBlocks()) {
      context.clear();
      this.visit(context, plan, block, block.getRoot(), new Stack<LogicalNode>());
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("=============================================");
      LOG.debug("FilterPushDown Optimized Query: \n" + plan.toString());
      LOG.debug("=============================================");
    }
    return plan;
  }

  @Override
  public LogicalNode visitFilter(FilterPushDownContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 SelectionNode selNode, Stack<LogicalNode> stack) throws PlanningException {
    context.pushingDownFilters.addAll(Sets.newHashSet(AlgebraicUtil.toConjunctiveNormalFormArray(selNode.getQual())));

    stack.push(selNode);
    visit(context, plan, block, selNode.getChild(), stack);
    stack.pop();

    if(context.pushingDownFilters.size() == 0) {
      // remove the selection operator if there is no search condition after selection push.
      LogicalNode node = stack.peek();
      if (node instanceof UnaryNode) {
        UnaryNode unary = (UnaryNode) node;
        unary.setChild(selNode.getChild());
      } else {
        throw new InvalidQueryException("Unexpected Logical Query Plan");
      }
    } else { // if there remain search conditions

      // check if it can be evaluated here
      Set<EvalNode> matched = TUtil.newHashSet();
      for (EvalNode eachEval : context.pushingDownFilters) {
        if (LogicalPlanner.checkIfBeEvaluatedAtThis(eachEval, selNode)) {
          matched.add(eachEval);
        }
      }

      // if there are search conditions which can be evaluated here,
      // push down them and remove them from context.pushingDownFilters.
      if (matched.size() > 0) {
        selNode.setQual(AlgebraicUtil.createSingletonExprFromCNF(matched.toArray(new EvalNode[matched.size()])));
        context.pushingDownFilters.removeAll(matched);
      }
    }

    return selNode;
  }

  @Override
  public LogicalNode visitJoin(FilterPushDownContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, JoinNode joinNode,
                               Stack<LogicalNode> stack) throws PlanningException {
    LogicalNode left = joinNode.getRightChild();
    LogicalNode right = joinNode.getLeftChild();

    // here we should stop selection pushdown on the null supplying side(s) of an outer join
    // get the two operands of the join operation as well as the join type
    JoinType joinType = joinNode.getJoinType();
    EvalNode joinQual = joinNode.getJoinQual();
    if (joinQual != null && LogicalPlanner.isOuterJoin(joinType)) {
      BinaryEval binaryEval = (BinaryEval) joinQual;
      // if both are fields
      if (binaryEval.getLeftExpr().getType() == EvalType.FIELD &&
          binaryEval.getRightExpr().getType() == EvalType.FIELD) {

        String leftTableName = ((FieldEval) binaryEval.getLeftExpr()).getQualifier();
        String rightTableName = ((FieldEval) binaryEval.getRightExpr()).getQualifier();
        List<String> nullSuppliers = Lists.newArrayList();
        Set<String> leftTableSet = Sets.newHashSet(PlannerUtil.getRelationLineageWithinQueryBlock(plan,
            joinNode.getLeftChild()));
        Set<String> rightTableSet = Sets.newHashSet(PlannerUtil.getRelationLineageWithinQueryBlock(plan,
            joinNode.getRightChild()));

        // some verification
        if (joinType == JoinType.FULL_OUTER) {
          nullSuppliers.add(leftTableName);
          nullSuppliers.add(rightTableName);

          // verify that these null suppliers are indeed in the left and right sets
          if (!rightTableSet.contains(nullSuppliers.get(0)) && !leftTableSet.contains(nullSuppliers.get(0))) {
            throw new InvalidQueryException("Incorrect Logical Query Plan with regard to outer join");
          }
          if (!rightTableSet.contains(nullSuppliers.get(1)) && !leftTableSet.contains(nullSuppliers.get(1))) {
            throw new InvalidQueryException("Incorrect Logical Query Plan with regard to outer join");
          }

        } else if (joinType == JoinType.LEFT_OUTER) {
          nullSuppliers.add(((RelationNode)joinNode.getRightChild()).getCanonicalName());
          //verify that this null supplier is indeed in the right sub-tree
          if (!rightTableSet.contains(nullSuppliers.get(0))) {
            throw new InvalidQueryException("Incorrect Logical Query Plan with regard to outer join");
          }
        } else if (joinType == JoinType.RIGHT_OUTER) {
          if (((RelationNode)joinNode.getRightChild()).getCanonicalName().equals(rightTableName)) {
            nullSuppliers.add(leftTableName);
          } else {
            nullSuppliers.add(rightTableName);
          }

          // verify that this null supplier is indeed in the left sub-tree
          if (!leftTableSet.contains(nullSuppliers.get(0))) {
            throw new InvalidQueryException("Incorrect Logical Query Plan with regard to outer join");
          }
        }
      }
    }

    // get evals from ON clause
    List<EvalNode> onConditions = new ArrayList<EvalNode>();
    if (joinNode.hasJoinQual()) {
      onConditions.addAll(Sets.newHashSet(AlgebraicUtil.toConjunctiveNormalFormArray(joinNode.getJoinQual())));
    }

    boolean isTopMostJoin = stack.peek().getType() != NodeType.JOIN;

    List<EvalNode> outerJoinPredicationEvals = new ArrayList<EvalNode>();
    List<EvalNode> outerJoinFilterEvalsExcludePredication = new ArrayList<EvalNode>();
    if (LogicalPlanner.isOuterJoin(joinNode.getJoinType())) {
      // TAJO-853
      // In the case of top most JOIN, all filters except JOIN condition aren't pushed down.
      // That filters are processed by SELECTION NODE.
      Set<String> nullSupplyingTableNameSet;
      if (joinNode.getJoinType() == JoinType.RIGHT_OUTER) {
        nullSupplyingTableNameSet = TUtil.newHashSet(PlannerUtil.getRelationLineage(joinNode.getLeftChild()));
      } else {
        nullSupplyingTableNameSet = TUtil.newHashSet(PlannerUtil.getRelationLineage(joinNode.getRightChild()));
      }

      for (EvalNode eachEval: context.pushingDownFilters) {
//        if (isTopMostJoin && !EvalTreeUtil.isJoinQual(eachEval, true)) {
//          outerJoinFilterEvalsExcludePredication.add(eachEval);
//        } else {
//          outerJoinPredicationEvals.add(eachEval);
//        }
        if (EvalTreeUtil.isJoinQual(eachEval, true)) {
          outerJoinPredicationEvals.add(eachEval);
        } else {
          Set<Column> columns = EvalTreeUtil.findUniqueColumns(eachEval);
          boolean canPushDown = true;
          for (Column eachColumn: columns) {
            if (nullSupplyingTableNameSet.contains(eachColumn.getQualifier())) {
              canPushDown = false;
              break;
            }
          }
          if (canPushDown) {
            context.pushingDownFilters.add(eachEval);
          } else {
            outerJoinFilterEvalsExcludePredication.add(eachEval);
          }
        }
      }

//      context.pushingDownFilters.removeAll(outerJoinFilterEvalsExcludePredication);

      Set<String> preservedTableNameSet;
      if (joinNode.getJoinType() == JoinType.RIGHT_OUTER) {
        preservedTableNameSet = TUtil.newHashSet(PlannerUtil.getRelationLineage(joinNode.getRightChild()));
      } else {
        preservedTableNameSet = TUtil.newHashSet(PlannerUtil.getRelationLineage(joinNode.getLeftChild()));
      }
      for (EvalNode eachOnEval: onConditions) {
        if (EvalTreeUtil.isJoinQual(eachOnEval, true)) {
          // If join condition, processing in the JoinNode.
          outerJoinPredicationEvals.add(eachOnEval);
        } else {
          // If Eval has a column which belong to Preserved Row table, not using to push down but using JoinCondition
          Set<Column> columns = EvalTreeUtil.findUniqueColumns(eachOnEval);
          boolean canPushDown = true;
          for (Column eachColumn: columns) {
            if (preservedTableNameSet.contains(eachColumn.getQualifier())) {
              canPushDown = false;
              break;
            }
          }
          if (canPushDown) {
            context.pushingDownFilters.add(eachOnEval);
          } else {
            outerJoinPredicationEvals.add(eachOnEval);
          }
        }
      }
    } else {
      context.pushingDownFilters.addAll(onConditions);
    }

    List<EvalNode> notMatched = new ArrayList<EvalNode>();
    // Join's input schema = right child output columns + left child output columns
    Map<EvalNode, EvalNode> transformedMap = findCanPushdownAndTransform(context, joinNode, left, notMatched, true,
        right.getOutSchema().size());
    context.setFiltersTobePushed(transformedMap.keySet());
    visit(context, plan, block, left, stack);

    context.setToOrigin(transformedMap);
    context.addFiltersTobePushed(notMatched);

    transformedMap = findCanPushdownAndTransform(context, joinNode, right, notMatched, true, 0);
    context.setFiltersTobePushed(new HashSet<EvalNode>(transformedMap.keySet()));

    visit(context, plan, block, right, stack);

    context.setToOrigin(transformedMap);
    context.addFiltersTobePushed(notMatched);

    List<EvalNode> matched = Lists.newArrayList();
    if(LogicalPlanner.isOuterJoin(joinNode.getJoinType())) {
      matched.addAll(outerJoinPredicationEvals);
    } else {
      for (EvalNode eval : context.pushingDownFilters) {
        if (LogicalPlanner.checkIfBeEvaluatedAtJoin(block, eval, joinNode, isTopMostJoin)) {
          matched.add(eval);
        }
      }
    }

    EvalNode qual = null;
    if (matched.size() > 1) {
      // merged into one eval tree
      qual = AlgebraicUtil.createSingletonExprFromCNF(
          matched.toArray(new EvalNode[matched.size()]));
    } else if (matched.size() == 1) {
      // if the number of matched expr is one
      qual = matched.get(0);
    }

    if (qual != null) {
      joinNode.setJoinQual(qual);

      if (joinNode.getJoinType() == JoinType.CROSS) {
        joinNode.setJoinType(JoinType.INNER);
      }
      context.pushingDownFilters.removeAll(matched);
    }

    context.pushingDownFilters.addAll(outerJoinFilterEvalsExcludePredication);
    return joinNode;
  }

  private Map<EvalNode, EvalNode> transformEvalsWidthByPassNode(
      Collection<EvalNode> originEvals, LogicalPlan plan,
      LogicalPlan.QueryBlock block,
      LogicalNode node, LogicalNode childNode) throws PlanningException {
    // transformed -> pushingDownFilters
    Map<EvalNode, EvalNode> transformedMap = new HashMap<EvalNode, EvalNode>();

    if (originEvals.isEmpty()) {
      return transformedMap;
    }

    if (node.getType() == NodeType.UNION) {
      // If node is union, All eval's columns are simple name and matched with child's output schema.
      Schema childOutSchema = childNode.getOutSchema();
      for (EvalNode eval : originEvals) {
        EvalNode copy;
        try {
          copy = (EvalNode) eval.clone();
        } catch (CloneNotSupportedException e) {
          throw new PlanningException(e);
        }

        Set<Column> columns = EvalTreeUtil.findUniqueColumns(copy);
        for (Column c : columns) {
          Column column = childOutSchema.getColumn(c.getSimpleName());
          if (column == null) {
            throw new PlanningException(
                "Invalid Filter PushDown on SubQuery: No such a corresponding column '"
                    + c.getQualifiedName() + " for FilterPushDown(" + eval + "), " +
                    "(PID=" + node.getPID() + ", Child=" + childNode.getPID() + ")");
          }
          EvalTreeUtil.changeColumnRef(copy, c.getSimpleName(), column.getQualifiedName());
        }

        transformedMap.put(copy, eval);
      }
      return transformedMap;
    }

    if (childNode.getType() == NodeType.UNION) {
      // If child is union, remove qualifier from eval's column
      for (EvalNode eval : originEvals) {
        EvalNode copy;
        try {
          copy = (EvalNode) eval.clone();
        } catch (CloneNotSupportedException e) {
          throw new PlanningException(e);
        }

        Set<Column> columns = EvalTreeUtil.findUniqueColumns(copy);
        for (Column c : columns) {
          if (c.hasQualifier()) {
            EvalTreeUtil.changeColumnRef(copy, c.getQualifiedName(), c.getSimpleName());
          }
        }

        transformedMap.put(copy, eval);
      }

      return transformedMap;
    }

    // node in column -> child out column
    Map<String, String> columnMap = new HashMap<String, String>();

    for (int i = 0; i < node.getInSchema().size(); i++) {
      String inColumnName = node.getInSchema().getColumn(i).getQualifiedName();
      Column childOutColumn = childNode.getOutSchema().getColumn(i);
      columnMap.put(inColumnName, childOutColumn.getQualifiedName());
    }

    // Rename from upper block's one to lower block's one
    for (EvalNode matchedEval : originEvals) {
      EvalNode copy;
      try {
        copy = (EvalNode) matchedEval.clone();
      } catch (CloneNotSupportedException e) {
        throw new PlanningException(e);
      }

      Set<Column> columns = EvalTreeUtil.findUniqueColumns(copy);
      boolean allMatched = true;
      for (Column c : columns) {
        if (columnMap.containsKey(c.getQualifiedName())) {
          EvalTreeUtil.changeColumnRef(copy, c.getQualifiedName(), columnMap.get(c.getQualifiedName()));
        } else {
          if (childNode.getType() == NodeType.GROUP_BY) {
            if (((GroupbyNode) childNode).isAggregationColumn(c.getSimpleName())) {
              allMatched = false;
              break;
            }
          } else {
            throw new PlanningException(
                "Invalid Filter PushDown on SubQuery: No such a corresponding column '"
                    + c.getQualifiedName() + " for FilterPushDown(" + matchedEval + "), " +
                    "(PID=" + node.getPID() + ", Child=" + childNode.getPID() + ")"
            );
          }
        }
      }
      if (allMatched) {
        transformedMap.put(copy, matchedEval);
      }
    }

    return transformedMap;
  }

  @Override
  public LogicalNode visitTableSubQuery(FilterPushDownContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                        TableSubQueryNode node, Stack<LogicalNode> stack) throws PlanningException {
    List<EvalNode> matched = Lists.newArrayList();
    for (EvalNode eval : context.pushingDownFilters) {
      if (LogicalPlanner.checkIfBeEvaluatedAtRelation(block, eval, node)) {
        matched.add(eval);
      }
    }

    // transformed -> pushingDownFilters
    Map<EvalNode, EvalNode> transformedMap =
        transformEvalsWidthByPassNode(matched, plan, block, node, node.getSubQuery());

    context.setFiltersTobePushed(new HashSet<EvalNode>(transformedMap.keySet()));
    visit(context, plan, plan.getBlock(node.getSubQuery()));

    context.setToOrigin(transformedMap);

    return node;
  }

  @Override
  public LogicalNode visitUnion(FilterPushDownContext context, LogicalPlan plan,
                                LogicalPlan.QueryBlock block, UnionNode unionNode,
                                Stack<LogicalNode> stack) throws PlanningException {
    LogicalNode leftNode = unionNode.getLeftChild();

    List<EvalNode> origins = new ArrayList<EvalNode>(context.pushingDownFilters);

    // transformed -> pushingDownFilters
    Map<EvalNode, EvalNode> transformedMap = transformEvalsWidthByPassNode(origins, plan, block, unionNode, leftNode);
    context.setFiltersTobePushed(new HashSet<EvalNode>(transformedMap.keySet()));
    visit(context, plan, plan.getBlock(leftNode));

    if (!context.pushingDownFilters.isEmpty()) {
      errorFilterPushDown(plan, leftNode, context);
    }

    LogicalNode rightNode = unionNode.getRightChild();
    transformedMap = transformEvalsWidthByPassNode(origins, plan, block, unionNode, rightNode);
    context.setFiltersTobePushed(new HashSet<EvalNode>(transformedMap.keySet()));
    visit(context, plan, plan.getBlock(rightNode), rightNode, stack);

    if (!context.pushingDownFilters.isEmpty()) {
      errorFilterPushDown(plan, rightNode, context);
    }

    // notify all filter matched to upper
    context.pushingDownFilters.clear();
    return unionNode;
  }

  @Override
  public LogicalNode visitProjection(FilterPushDownContext context,
                                     LogicalPlan plan,
                                     LogicalPlan.QueryBlock block,
                                     ProjectionNode projectionNode,
                                     Stack<LogicalNode> stack) throws PlanningException {
    LogicalNode childNode = projectionNode.getChild();

    List<EvalNode> notMatched = new ArrayList<EvalNode>();

    //copy -> origin
    Map<EvalNode, EvalNode> matched = findCanPushdownAndTransform(
        context, projectionNode, childNode, notMatched, false, 0);

    context.setFiltersTobePushed(matched.keySet());

    stack.push(projectionNode);
    LogicalNode current = visit(context, plan, plan.getBlock(childNode), childNode, stack);
    stack.pop();

    // find not matched after visiting child
    for (EvalNode eval: context.pushingDownFilters) {
      notMatched.add(matched.get(eval));
    }

    EvalNode qual = null;
    if (notMatched.size() > 1) {
      // merged into one eval tree
      qual = AlgebraicUtil.createSingletonExprFromCNF(notMatched.toArray(new EvalNode[notMatched.size()]));
    } else if (notMatched.size() == 1) {
      // if the number of matched expr is one
      qual = notMatched.get(0);
    }

    // If there is not matched node add SelectionNode and clear context.pushingDownFilters
    if (qual != null) {
      SelectionNode selectionNode = plan.createNode(SelectionNode.class);
      selectionNode.setInSchema(current.getOutSchema());
      selectionNode.setOutSchema(current.getOutSchema());
      selectionNode.setQual(qual);
      block.registerNode(selectionNode);

      projectionNode.setChild(selectionNode);
      selectionNode.setChild(current);
    }

    //notify all eval matched to upper
    context.pushingDownFilters.clear();

    return current;
  }

  private Map<EvalNode, EvalNode> findCanPushdownAndTransform(
      FilterPushDownContext context, Projectable node,
      LogicalNode childNode, List<EvalNode> notMatched,
      boolean ignoreJoin, int columnOffset) throws PlanningException {
    // canonical name -> target
    Map<String, Target> nodeTargetMap = new HashMap<String, Target>();
    for (Target target : node.getTargets()) {
      nodeTargetMap.put(target.getCanonicalName(), target);
    }

    // copy -> origin
    Map<EvalNode, EvalNode> matched = new HashMap<EvalNode, EvalNode>();

    for (EvalNode eval : context.pushingDownFilters) {
      if (ignoreJoin && EvalTreeUtil.isJoinQual(eval, true)) {
        notMatched.add(eval);
        continue;
      }
      // If all column is field eval, can push down.
      Set<Column> evalColumns = EvalTreeUtil.findUniqueColumns(eval);
      boolean columnMatched = true;
      for (Column c : evalColumns) {
        Target target = nodeTargetMap.get(c.getQualifiedName());
        if (target == null) {
          columnMatched = false;
          break;
        }
        if (target.getEvalTree().getType() != EvalType.FIELD) {
          columnMatched = false;
          break;
        }
      }

      if (columnMatched) {
        // transform eval column to child's output column
        EvalNode copyEvalNode = transformEval(node, childNode, eval, nodeTargetMap, columnOffset);
        if (copyEvalNode != null) {
          matched.put(copyEvalNode, eval);
        } else {
          notMatched.add(eval);
        }
      } else {
        notMatched.add(eval);
      }
    }

    return matched;
  }

  private EvalNode transformEval(Projectable node, LogicalNode childNode, EvalNode origin,
                                 Map<String, Target> targetMap, int columnOffset) throws PlanningException {
    Schema outputSchema = childNode != null ? childNode.getOutSchema() : node.getInSchema();
    EvalNode copy;
    try {
      copy = (EvalNode) origin.clone();
    } catch (CloneNotSupportedException e) {
      throw new PlanningException(e);
    }
    Set<Column> columns = EvalTreeUtil.findUniqueColumns(copy);
    for (Column c: columns) {
      Target target = targetMap.get(c.getQualifiedName());
      if (target == null) {
        throw new PlanningException(
            "Invalid Filter PushDown: No such a corresponding target '"
                + c.getQualifiedName() + " for FilterPushDown(" + origin + "), " +
                "(PID=" + node.getPID() + ")"
        );
      }
      EvalNode targetEvalNode = target.getEvalTree();
      if (targetEvalNode.getType() != EvalType.FIELD) {
        throw new PlanningException(
            "Invalid Filter PushDown: '" + c.getQualifiedName() + "' target is not FieldEval " +
                "(PID=" + node.getPID() + ")"
        );
      }

      FieldEval fieldEval = (FieldEval)targetEvalNode;
      Column targetInputColumn = fieldEval.getColumnRef();

      int index;
      if (targetInputColumn.hasQualifier()) {
        index = node.getInSchema().getColumnId(targetInputColumn.getQualifiedName());
      } else {
        index = node.getInSchema().getColumnIdByName(targetInputColumn.getQualifiedName());
      }
      if (columnOffset > 0) {
        index = index - columnOffset;
      }
      if (index < 0 || index >= outputSchema.size()) {
        return null;
      }
      Column outputColumn = outputSchema.getColumn(index);

      EvalTreeUtil.changeColumnRef(copy, c.getQualifiedName(), outputColumn.getQualifiedName());
    }

    return copy;
  }

  /**
   * Find aggregation columns in filter eval and add having clause or add HavingNode.
   * @param context
   * @param plan
   * @param block
   * @param parentNode  If null, having is parent
   * @param havingNode      If null, projection is parent
   * @param groupByNode
   * @return matched origin eval
   * @throws PlanningException
   */
  private List<EvalNode> addHavingNode(FilterPushDownContext context, LogicalPlan plan,
                                       LogicalPlan.QueryBlock block,
                                       UnaryNode parentNode,
                                       HavingNode havingNode,
                                       GroupbyNode groupByNode) throws PlanningException {
    // find aggregation column
    Set<Column> groupingColumns = new HashSet<Column>(Arrays.asList(groupByNode.getGroupingColumns()));
    Set<String> aggrFunctionOutColumns = new HashSet<String>();
    for (Column column : groupByNode.getOutSchema().getColumns()) {
      if (!groupingColumns.contains(column)) {
        aggrFunctionOutColumns.add(column.getQualifiedName());
      }
    }

    List<EvalNode> aggrEvalOrigins = new ArrayList<EvalNode>();
    List<EvalNode> aggrEvals = new ArrayList<EvalNode>();

    for (EvalNode eval : context.pushingDownFilters) {
      EvalNode copy = null;
      try {
        copy = (EvalNode)eval.clone();
      } catch (CloneNotSupportedException e) {
      }
      boolean isEvalAggrFunction = false;
      for (Column evalColumn : EvalTreeUtil.findUniqueColumns(copy)) {
        if (aggrFunctionOutColumns.contains(evalColumn.getSimpleName())) {
          EvalTreeUtil.changeColumnRef(copy, evalColumn.getQualifiedName(), evalColumn.getSimpleName());
          isEvalAggrFunction = true;
          break;
        }
      }
      if (isEvalAggrFunction) {
        aggrEvals.add(copy);
        aggrEvalOrigins.add(eval);
      }
    }

    if (aggrEvals.isEmpty()) {
      return aggrEvalOrigins;
    }

    // transform

    HavingNode workingHavingNode;
    if (havingNode != null) {
      workingHavingNode = havingNode;
      aggrEvals.add(havingNode.getQual());
    } else {
      workingHavingNode = plan.createNode(HavingNode.class);
      block.registerNode(workingHavingNode);
      parentNode.setChild(workingHavingNode);
      workingHavingNode.setChild(groupByNode);
    }

    EvalNode qual = null;
    if (aggrEvals.size() > 1) {
      // merged into one eval tree
      qual = AlgebraicUtil.createSingletonExprFromCNF(aggrEvals.toArray(new EvalNode[aggrEvals.size()]));
    } else if (aggrEvals.size() == 1) {
      // if the number of matched expr is one
      qual = aggrEvals.get(0);
    }

    // If there is not matched node add SelectionNode and clear context.pushingDownFilters
    if (qual != null) {
      workingHavingNode.setQual(qual);
    }

    return aggrEvalOrigins;
  }

  @Override
  public LogicalNode visitGroupBy(FilterPushDownContext context, LogicalPlan plan,
                                  LogicalPlan.QueryBlock block, GroupbyNode groupbyNode,
                                  Stack<LogicalNode> stack) throws PlanningException {
    LogicalNode parentNode = stack.peek();
    List<EvalNode> aggrEvals;
    if (parentNode.getType() == NodeType.HAVING) {
      aggrEvals = addHavingNode(context, plan, block, null, (HavingNode)parentNode, groupbyNode);
    } else {
      aggrEvals = addHavingNode(context, plan, block, (UnaryNode)parentNode, null, groupbyNode);
    }

    if (aggrEvals != null) {
      // remove aggregation eval from conext
      context.pushingDownFilters.removeAll(aggrEvals);
    }

    List<EvalNode> notMatched = new ArrayList<EvalNode>();
    // transform
    Map<EvalNode, EvalNode> tranformed =
        findCanPushdownAndTransform(context, groupbyNode,groupbyNode.getChild(), notMatched, false, 0);

    context.setFiltersTobePushed(tranformed.keySet());
    LogicalNode current = super.visitGroupBy(context, plan, block, groupbyNode, stack);

    context.setToOrigin(tranformed);
    context.addFiltersTobePushed(notMatched);

    return current;
  }

  @Override
  public LogicalNode visitScan(FilterPushDownContext context, LogicalPlan plan,
                               LogicalPlan.QueryBlock block, ScanNode scanNode,
                               Stack<LogicalNode> stack) throws PlanningException {
    List<EvalNode> matched = Lists.newArrayList();

    // find partition column and check matching
    Set<String> partitionColumns = new HashSet<String>();
    TableDesc table = scanNode.getTableDesc();
    if (table.hasPartition()) {
      for (Column c: table.getPartitionMethod().getExpressionSchema().getColumns()) {
        partitionColumns.add(c.getQualifiedName());
      }
    }
    Set<EvalNode> partitionEvals = new HashSet<EvalNode>();
    for (EvalNode eval : context.pushingDownFilters) {
      if (table.hasPartition()) {
        Set<Column> columns = EvalTreeUtil.findUniqueColumns(eval);
        if (columns.size() != 1) {
          continue;
        }
        Column column = columns.iterator().next();

        if (partitionColumns.contains(column.getSimpleName())) {
          EvalNode copy;
          try {
            copy = (EvalNode) eval.clone();
          } catch (CloneNotSupportedException e) {
            throw new PlanningException(e);
          }
          EvalTreeUtil.changeColumnRef(copy, column.getQualifiedName(),
              scanNode.getCanonicalName() + "." + column.getSimpleName());
          matched.add(copy);
          partitionEvals.add(eval);
        }
      }
    }

    context.pushingDownFilters.removeAll(partitionEvals);

    List<EvalNode> notMatched = new ArrayList<EvalNode>();

    // transform
    Map<EvalNode, EvalNode> transformed =
        findCanPushdownAndTransform(context, scanNode, null, notMatched, true, 0);

    for (EvalNode eval : transformed.keySet()) {
      if (LogicalPlanner.checkIfBeEvaluatedAtRelation(block, eval, scanNode)) {
        matched.add(eval);
      }
    }

    EvalNode qual = null;
    if (matched.size() > 1) {
      // merged into one eval tree
      qual = AlgebraicUtil.createSingletonExprFromCNF(
          matched.toArray(new EvalNode[matched.size()]));
    } else if (matched.size() == 1) {
      // if the number of matched expr is one
      qual = matched.iterator().next();
    }

    if (qual != null) { // if a matched qual exists
      scanNode.setQual(qual);
    }

    for (EvalNode matchedEval: matched) {
      transformed.remove(matchedEval);
    }

    context.setToOrigin(transformed);
    context.addFiltersTobePushed(notMatched);

    return scanNode;
  }

  private void errorFilterPushDown(LogicalPlan plan, LogicalNode node,
                                   FilterPushDownContext context) throws PlanningException {
    String notMatchedNodeStr = "";
    String prefix = "";
    for (EvalNode notMatchedNode: context.pushingDownFilters) {
      notMatchedNodeStr += prefix + notMatchedNode;
      prefix = ", ";
    }
    throw new PlanningException("FilterPushDown failed cause some filters not matched: " + notMatchedNodeStr + "\n" +
        "Error node: " + node.getPlanString() + "\n" +
        plan.toString());
  }
}
