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

package org.apache.tajo.plan.rewrite.rules;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.SessionVars;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.*;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlan.QueryBlock;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRule;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRuleContext;
import org.apache.tajo.plan.rewrite.rules.FilterPushDownRule.FilterPushDownContext;
import org.apache.tajo.plan.rewrite.rules.IndexScanInfo.SimplePredicate;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;
import org.apache.tajo.util.TUtil;

import java.util.*;

/**
 * This rule tries to push down all filter conditions into logical nodes as lower as possible.
 * It is likely to significantly reduces the intermediate data.
 */
public class FilterPushDownRule extends BasicLogicalPlanVisitor<FilterPushDownContext, LogicalNode>
    implements LogicalPlanRewriteRule {
  private final static Log LOG = LogFactory.getLog(FilterPushDownRule.class);
  private static final String NAME = "FilterPushDown";

  private CatalogService catalog;

  static class FilterPushDownContext {
    Set<EvalNode> pushingDownFilters = new HashSet<>();
    LogicalPlanRewriteRuleContext rewriteRuleContext;

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
      List<EvalNode> origins = TUtil.newList();
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
  public boolean isEligible(LogicalPlanRewriteRuleContext context) {
    for (LogicalPlan.QueryBlock block : context.getPlan().getQueryBlocks()) {
      if (block.hasNode(NodeType.SELECTION) || block.hasNode(NodeType.JOIN)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public LogicalPlan rewrite(LogicalPlanRewriteRuleContext rewriteRuleContext) throws TajoException {
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
    context.rewriteRuleContext = rewriteRuleContext;
    LogicalPlan plan = rewriteRuleContext.getPlan();
    catalog = rewriteRuleContext.getCatalog();
    for (LogicalPlan.QueryBlock block : plan.getQueryBlocks()) {
      context.clear();
      this.visit(context, plan, block, block.getRoot(), new Stack<>());
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
                                 SelectionNode selNode, Stack<LogicalNode> stack) throws TajoException {
    context.pushingDownFilters.addAll(new HashSet<>(Arrays.asList(AlgebraicUtil.toConjunctiveNormalFormArray(selNode.getQual()))));

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
        throw new TajoInternalError("The node must be an unary node");
      }
    } else { // if there remain search conditions

      // check if it can be evaluated here
      Set<EvalNode> matched = new HashSet<>();
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
  public LogicalNode visitJoin(FilterPushDownContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                               JoinNode joinNode, Stack<LogicalNode> stack) throws TajoException {
    Set<EvalNode> onPredicates = new HashSet<>();
    if (joinNode.hasJoinQual()) {
      onPredicates.addAll(new HashSet<>(Arrays.asList(AlgebraicUtil.toConjunctiveNormalFormArray(joinNode.getJoinQual()))));
    }
    // clear join qual
    joinNode.clearJoinQual();

    // we assume all the quals in pushingDownFilters as where predicates
    Set<EvalNode> nonPushableQuals = extractNonPushableJoinQuals(plan, block, joinNode, onPredicates,
        context.pushingDownFilters);
    // add every predicate and remove non-pushable ones
    context.pushingDownFilters.addAll(onPredicates);
    context.pushingDownFilters.removeAll(nonPushableQuals);

    LogicalNode left = joinNode.getLeftChild();
    LogicalNode right = joinNode.getRightChild();

    List<EvalNode> notMatched = TUtil.newList();
    // Join's input schema = right child output columns + left child output columns
    Map<EvalNode, EvalNode> transformedMap = findCanPushdownAndTransform(context, block, joinNode, left, notMatched,
        null, 0);
    context.setFiltersTobePushed(transformedMap.keySet());
    visit(context, plan, block, left, stack);

    context.setToOrigin(transformedMap);
    context.addFiltersTobePushed(notMatched);

    notMatched.clear();
    transformedMap = findCanPushdownAndTransform(context, block, joinNode, right, notMatched, null,
        left.getOutSchema().size());
    context.setFiltersTobePushed(transformedMap.keySet());

    visit(context, plan, block, right, stack);

    context.setToOrigin(transformedMap);
    context.addFiltersTobePushed(notMatched);

    notMatched.clear();
    context.pushingDownFilters.addAll(nonPushableQuals);
    List<EvalNode> matched = TUtil.newList();

    // If the query involves a subquery, the stack can be empty.
    // In this case, this join is the top most one within a query block.
    boolean isTopMostJoin = stack.isEmpty() ? true : stack.peek().getType() != NodeType.JOIN;

    for (EvalNode evalNode : context.pushingDownFilters) {
      // TODO: currently, non-equi theta join is not supported yet.
      if (LogicalPlanner.isEvaluatableJoinQual(block, evalNode, joinNode, onPredicates.contains(evalNode),
          isTopMostJoin)) {
        matched.add(evalNode);
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
    }

    context.pushingDownFilters.removeAll(matched);

    // The selection node for non-equi theta join conditions should be created here
    // to process those conditions as early as possible.
    // This should be removed after TAJO-742.
    if (context.pushingDownFilters.size() > 0) {
      List<EvalNode> nonEquiThetaJoinQuals = extractNonEquiThetaJoinQuals(context.pushingDownFilters, block, joinNode);
      if (nonEquiThetaJoinQuals.size() > 0) {
        SelectionNode selectionNode = createSelectionParentForNonEquiThetaJoinQuals(plan, block, stack, joinNode,
            nonEquiThetaJoinQuals);

        context.pushingDownFilters.removeAll(nonEquiThetaJoinQuals);
        return selectionNode;
      }
    }

    return joinNode;
  }

  private SelectionNode createSelectionParentForNonEquiThetaJoinQuals(LogicalPlan plan,
                                                                      QueryBlock block,
                                                                      Stack<LogicalNode> stack,
                                                                      JoinNode joinNode,
                                                                      List<EvalNode> nonEquiThetaJoinQuals) {
    SelectionNode selectionNode = plan.createNode(SelectionNode.class);
    selectionNode.setInSchema(joinNode.getOutSchema());
    selectionNode.setOutSchema(joinNode.getOutSchema());
    selectionNode.setQual(AlgebraicUtil.createSingletonExprFromCNF(nonEquiThetaJoinQuals));
    block.registerNode(selectionNode);

    LogicalNode parent = stack.peek();
    if (parent instanceof UnaryNode) {
      ((UnaryNode) parent).setChild(selectionNode);
    } else if (parent instanceof BinaryNode) {
      BinaryNode binaryParent = (BinaryNode) parent;
      if (binaryParent.getLeftChild().getPID() == joinNode.getPID()) {
        binaryParent.setLeftChild(selectionNode);
      } else if (binaryParent.getRightChild().getPID() == joinNode.getPID()) {
        binaryParent.setRightChild(selectionNode);
      }
    } else if (parent instanceof TableSubQueryNode) {
      ((TableSubQueryNode) parent).setSubQuery(selectionNode);
    }

    selectionNode.setChild(joinNode);
    return selectionNode;
  }

  private static Set<EvalNode> extractNonPushableJoinQuals(final LogicalPlan plan,
                                                           final LogicalPlan.QueryBlock block,
                                                           final JoinNode joinNode,
                                                           final Set<EvalNode> onPredicates,
                                                           final Set<EvalNode> wherePredicates)
      throws TajoException {
    Set<EvalNode> nonPushableQuals = new HashSet<>();
    // TODO: non-equi theta join quals must not be pushed until TAJO-742 is resolved.
    nonPushableQuals.addAll(extractNonEquiThetaJoinQuals(wherePredicates, block, joinNode));
    nonPushableQuals.addAll(extractNonEquiThetaJoinQuals(onPredicates, block, joinNode));

    // for outer joins
    if (PlannerUtil.isOuterJoinType(joinNode.getJoinType())) {
      nonPushableQuals.addAll(extractNonPushableOuterJoinQuals(plan, onPredicates, wherePredicates, joinNode));
    }
    return nonPushableQuals;
  }

  /**
   * For outer joins, pushable predicates can be decided based on their locations in the SQL and types of referencing
   * relations.
   *
   * <h3>Table types</h3>
   * <ul>
   *   <li>Preserved Row table : The preserved row table refers to the table that preserves rows when there is no match
   *   in the join operation. Therefore, all rows from the preserved row table that qualify against the WHERE clause
   *   will be returned, regardless of whether there is a matched row in the join. For a left/right table, the preserved
   *   row table is the left/right table. For a full outer join, both tables are preserved row tables.</li>
   *   <li>Null Supplying table : The NULL-supplying table supplies NULLs when there is an unmatched row. Any column
   *   from the NULL-supplying table referred to in the SELECT list or subsequent WHERE or ON clause will contain NULL
   *   if there was no match in the join operation. For a left/right outer join, the NULL-supplying
   *   table is the right/left table. For a full outer join, both tables are NULL-supplying
   *   table. In a full outer join, both tables can preserve rows, and also can supply NULLs. This is significant,
   *   because there are rules that apply to purely preserved row tables that do not apply if the table can also supply
   *   NULLs.</li>
   * </ul>
   *
   * <h3>Predicate types</h3>
   * <ul>
   *   <li>During Join predicate : A predicate that is in the JOIN ON clause.</li>
   *   <li>After Join predicate : A predicate that is in the WHERE clause.</li>
   * </ul>
   *
   * <h3>Predicate Pushdown Rules</h3>
   * <ol>
   *   <li>During Join predicates cannot be pushed past Preserved Row tables.</li>
   *   <li>After Join predicates cannot be pushed past Null Supplying tables.</li>
   * </ol>
   */
  private static Set<EvalNode> extractNonPushableOuterJoinQuals(final LogicalPlan plan,
                                                                final Set<EvalNode> onPredicates,
                                                                final Set<EvalNode> wherePredicates,
                                                                final JoinNode joinNode) throws TajoException {
    Set<String> nullSupplyingTableNameSet = new HashSet<>();
    Set<String> preservedTableNameSet = new HashSet<>();
    String leftRelation = PlannerUtil.getTopRelationInLineage(plan, joinNode.getLeftChild());
    String rightRelation = PlannerUtil.getTopRelationInLineage(plan, joinNode.getRightChild());

    if (joinNode.getJoinType() == JoinType.LEFT_OUTER) {
      nullSupplyingTableNameSet.add(rightRelation);
      preservedTableNameSet.add(leftRelation);
    } else if (joinNode.getJoinType() == JoinType.RIGHT_OUTER) {
      nullSupplyingTableNameSet.add(leftRelation);
      preservedTableNameSet.add(rightRelation);
    } else {
      // full outer join
      preservedTableNameSet.add(leftRelation);
      preservedTableNameSet.add(rightRelation);
      nullSupplyingTableNameSet.add(leftRelation);
      nullSupplyingTableNameSet.add(rightRelation);
    }

    Set<EvalNode> nonPushableQuals = new HashSet<>();
    for (EvalNode eachQual : onPredicates) {
      for (String relName : preservedTableNameSet) {
        if (isEvalNeedRelation(eachQual, relName)) {
          nonPushableQuals.add(eachQual);
        }
      }
    }

    for (EvalNode eachQual : wherePredicates) {
      for (String relName : nullSupplyingTableNameSet) {
        if (isEvalNeedRelation(eachQual, relName)) {
          nonPushableQuals.add(eachQual);
        }
      }
    }

    return nonPushableQuals;
  }

  private static boolean isEvalNeedRelation(final EvalNode evalNode, final String relationName) {
    Set<Column> columns = EvalTreeUtil.findUniqueColumns(evalNode);
    for (Column column : columns) {
      if (isColumnFromRelation(column, relationName)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isColumnFromRelation(final Column column, final String relationName) {
    if (relationName.equals(column.getQualifier())) {
      return true;
    }
    return false;
  }

  private static boolean isNonEquiThetaJoinQual(final LogicalPlan.QueryBlock block,
                                                final JoinNode joinNode,
                                                final EvalNode evalNode) {
    if (EvalTreeUtil.isJoinQual(block, joinNode.getLeftChild().getOutSchema(),
        joinNode.getRightChild().getOutSchema(), evalNode, true) &&
        evalNode.getType() != EvalType.EQUAL) {
      return true;
    } else {
      return false;
    }
  }

  private static List<EvalNode> extractNonEquiThetaJoinQuals(final Set<EvalNode> predicates,
                                                             final LogicalPlan.QueryBlock block,
                                                             final JoinNode joinNode) {
    List<EvalNode> nonEquiThetaJoinQuals = TUtil.newList();
    for (EvalNode eachEval: predicates) {
      if (isNonEquiThetaJoinQual(block, joinNode, eachEval)) {
        nonEquiThetaJoinQuals.add(eachEval);
      }
    }
    return nonEquiThetaJoinQuals;
  }

  private Map<EvalNode, EvalNode> transformEvalsWidthByPassNode(
      Collection<EvalNode> originEvals, LogicalPlan plan,
      LogicalPlan.QueryBlock block,
      LogicalNode node, LogicalNode childNode) throws TajoException {
    // transformed -> pushingDownFilters
    Map<EvalNode, EvalNode> transformedMap = new HashMap<>();

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
          throw new TajoInternalError(e);
        }

        Set<Column> columns = EvalTreeUtil.findUniqueColumns(copy);
        for (Column c : columns) {
          Column column = childOutSchema.getColumn(c.getSimpleName());
          if (column == null) {
            throw new TajoInternalError(
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
          throw new TajoInternalError(e);
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
    Map<String, String> columnMap = new HashMap<>();

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
        throw new TajoInternalError(e);
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
            throw new TajoInternalError(
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
                                        TableSubQueryNode node, Stack<LogicalNode> stack) throws TajoException {
    List<EvalNode> matched = TUtil.newList();
    List<EvalNode> unmatched = TUtil.newList();
    for (EvalNode eval : context.pushingDownFilters) {
      if (LogicalPlanner.checkIfBeEvaluatedAtRelation(block, eval, node)) {
        matched.add(eval);
      } else {
        unmatched.add(eval);
      }
    }

    // transformed -> pushingDownFilters
    Map<EvalNode, EvalNode> transformedMap =
        transformEvalsWidthByPassNode(matched, plan, block, node, node.getSubQuery());
    context.setFiltersTobePushed(transformedMap.keySet());
    visit(context, plan, plan.getBlock(node.getSubQuery()));
    context.setToOrigin(transformedMap);
    context.addFiltersTobePushed(unmatched);

    return node;
  }

  @Override
  public LogicalNode visitUnion(FilterPushDownContext context, LogicalPlan plan,
                                LogicalPlan.QueryBlock block, UnionNode unionNode,
                                Stack<LogicalNode> stack) throws TajoException {
    LogicalNode leftNode = unionNode.getLeftChild();

    List<EvalNode> origins = TUtil.newList(context.pushingDownFilters);

    // transformed -> pushingDownFilters
    Map<EvalNode, EvalNode> transformedMap = transformEvalsWidthByPassNode(origins, plan, block, unionNode, leftNode);
    context.setFiltersTobePushed(transformedMap.keySet());
    visit(context, plan, plan.getBlock(leftNode));

    if (!context.pushingDownFilters.isEmpty()) {
      errorFilterPushDown(plan, leftNode, context);
    }

    LogicalNode rightNode = unionNode.getRightChild();
    transformedMap = transformEvalsWidthByPassNode(origins, plan, block, unionNode, rightNode);
    context.setFiltersTobePushed(transformedMap.keySet());
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
                                     Stack<LogicalNode> stack) throws TajoException {
    LogicalNode childNode = projectionNode.getChild();

    List<EvalNode> notMatched = TUtil.newList();

    //copy -> origin
    BiMap<EvalNode, EvalNode> transformedMap = findCanPushdownAndTransform(
        context, block,projectionNode, childNode, notMatched, null, 0);

    context.setFiltersTobePushed(transformedMap.keySet());

    stack.push(projectionNode);
    visit(context, plan, plan.getBlock(childNode), childNode, stack);
    stack.pop();

    // find not matched after visiting child
    for (EvalNode eval: context.pushingDownFilters) {
      notMatched.add(transformedMap.get(eval));
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
    if (qual != null && LogicalPlanner.checkIfBeEvaluatedAtThis(qual, projectionNode)) {
      SelectionNode selectionNode = plan.createNode(SelectionNode.class);
      selectionNode.setInSchema(childNode.getOutSchema());
      selectionNode.setOutSchema(childNode.getOutSchema());
      selectionNode.setQual(qual);
      block.registerNode(selectionNode);

      projectionNode.setChild(selectionNode);
      selectionNode.setChild(childNode);

      // clean all remain filters because all conditions are merged into a qual
      context.pushingDownFilters.clear();
    }

    // if there are remain filters, recover the original names and give back to the upper query block.
    if (context.pushingDownFilters.size() > 0) {
      ImmutableSet<EvalNode> copy = ImmutableSet.copyOf(context.pushingDownFilters);
      context.pushingDownFilters.clear();
      context.pushingDownFilters.addAll(reverseTransform(transformedMap, copy));
    }

    return projectionNode;
  }

  private Collection<EvalNode> reverseTransform(BiMap<EvalNode, EvalNode> map, Set<EvalNode> remainFilters) {
    Set<EvalNode> reversed = new HashSet<>();
    for (EvalNode evalNode : remainFilters) {
      reversed.add(map.get(evalNode));
    }
    return reversed;
  }

  private BiMap<EvalNode, EvalNode> findCanPushdownAndTransform(
      FilterPushDownContext context, LogicalPlan.QueryBlock block, Projectable node,
      LogicalNode childNode, List<EvalNode> notMatched,
      Set<String> partitionColumns, int columnOffset) throws TajoException {
    // canonical name -> target
    Map<String, Target> nodeTargetMap = new HashMap<>();
    for (Target target : node.getTargets()) {
      nodeTargetMap.put(target.getCanonicalName(), target);
    }

    // copy -> origin
    BiMap<EvalNode, EvalNode> matched = HashBiMap.create();

    for (EvalNode eval : context.pushingDownFilters) {
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
        EvalNode copyEvalNode = transformEval(node, childNode, eval, nodeTargetMap, partitionColumns, columnOffset);
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
                                 Map<String, Target> targetMap, Set<String> partitionColumns,
                                 int columnOffset) throws TajoException {
    Schema outputSchema = childNode != null ? childNode.getOutSchema() : node.getInSchema();
    EvalNode copy;
    try {
      copy = (EvalNode) origin.clone();
    } catch (CloneNotSupportedException e) {
      throw new TajoInternalError(e);
    }
    Set<Column> columns = EvalTreeUtil.findUniqueColumns(copy);
    for (Column c: columns) {
      Target target = targetMap.get(c.getQualifiedName());
      if (target == null) {
        throw new TajoInternalError(
            "Invalid Filter PushDown: No such a corresponding target '"
                + c.getQualifiedName() + " for FilterPushDown(" + origin + "), " +
                "(PID=" + node.getPID() + ")"
        );
      }
      EvalNode targetEvalNode = target.getEvalTree();
      if (targetEvalNode.getType() != EvalType.FIELD) {
        throw new TajoInternalError(
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
        if (partitionColumns != null && !partitionColumns.isEmpty() && node instanceof ScanNode) {
          ScanNode scanNode = (ScanNode)node;
          boolean isPartitionColumn = false;
          if (CatalogUtil.isFQColumnName(partitionColumns.iterator().next())) {
            isPartitionColumn = partitionColumns.contains(
                CatalogUtil.buildFQName(scanNode.getTableName(), c.getSimpleName()));
          } else {
            isPartitionColumn = partitionColumns.contains(c.getSimpleName());
          }
          if (isPartitionColumn) {
            EvalTreeUtil.changeColumnRef(copy, c.getQualifiedName(),
                scanNode.getCanonicalName() + "." + c.getSimpleName());
          } else {
            return null;
          }
        } else {
          return null;
        }
      } else {
        Column outputColumn = outputSchema.getColumn(index);
        EvalTreeUtil.changeColumnRef(copy, c.getQualifiedName(), outputColumn.getQualifiedName());
      }
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
   * @throws TajoException
   */
  private List<EvalNode> addHavingNode(FilterPushDownContext context, LogicalPlan plan,
                                       LogicalPlan.QueryBlock block,
                                       UnaryNode parentNode,
                                       HavingNode havingNode,
                                       GroupbyNode groupByNode) throws TajoException {
    // find aggregation column
    Set<Column> groupingColumns = new HashSet<>(Arrays.asList(groupByNode.getGroupingColumns()));
    Set<String> aggrFunctionOutColumns = new HashSet<>();
    for (Column column : groupByNode.getOutSchema().getRootColumns()) {
      if (!groupingColumns.contains(column)) {
        aggrFunctionOutColumns.add(column.getQualifiedName());
      }
    }

    List<EvalNode> aggrEvalOrigins = TUtil.newList();
    List<EvalNode> aggrEvals = TUtil.newList();

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
      if (groupByNode.getInSchema().containsAll(EvalTreeUtil.findUniqueColumns(copy))) {
        isEvalAggrFunction = true;
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
  public LogicalNode visitWindowAgg(FilterPushDownContext context, LogicalPlan plan,
                                  LogicalPlan.QueryBlock block, WindowAggNode winAggNode,
                                  Stack<LogicalNode> stack) throws TajoException {
    stack.push(winAggNode);
    super.visitWindowAgg(context, plan, block, winAggNode, stack);
    stack.pop();
    return winAggNode;
  }


  @Override
  public LogicalNode visitGroupBy(FilterPushDownContext context, LogicalPlan plan,
                                  LogicalPlan.QueryBlock block, GroupbyNode groupbyNode,
                                  Stack<LogicalNode> stack) throws TajoException {
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

    List<EvalNode> notMatched = TUtil.newList();
    // transform
    Map<EvalNode, EvalNode> transformed =
        findCanPushdownAndTransform(context, block, groupbyNode,groupbyNode.getChild(), notMatched, null, 0);

    context.setFiltersTobePushed(transformed.keySet());
    LogicalNode current = super.visitGroupBy(context, plan, block, groupbyNode, stack);

    context.setToOrigin(transformed);
    context.addFiltersTobePushed(notMatched);

    return current;
  }

  @Override
  public LogicalNode visitScan(FilterPushDownContext context, LogicalPlan plan,
                               LogicalPlan.QueryBlock block, final ScanNode scanNode,
                               Stack<LogicalNode> stack) throws TajoException {
    List<EvalNode> matched = TUtil.newList();

    // find partition column and check matching
    Set<String> partitionColumns = new HashSet<>();
    TableDesc table = scanNode.getTableDesc();
    boolean hasQualifiedName = false;
    if (table.hasPartition()) {
      for (Column c: table.getPartitionMethod().getExpressionSchema().getRootColumns()) {
        partitionColumns.add(c.getQualifiedName());
        hasQualifiedName = c.hasQualifier();
      }
    }
    Set<EvalNode> partitionEvals = new HashSet<>();
    for (EvalNode eval : context.pushingDownFilters) {
      if (table.hasPartition()) {
        Set<Column> columns = EvalTreeUtil.findUniqueColumns(eval);
        if (columns.size() != 1) {
          continue;
        }
        Column column = columns.iterator().next();

        // If catalog runs with HiveCatalogStore, partition column is a qualified name
        // Else partition column is a simple name
        boolean isPartitionColumn = false;
        if (hasQualifiedName) {
          isPartitionColumn = partitionColumns.contains(CatalogUtil.buildFQName(table.getName(), column.getSimpleName()));
        } else {
          isPartitionColumn = partitionColumns.contains(column.getSimpleName());
        }
        if (isPartitionColumn) {
          EvalNode copy;
          try {
            copy = (EvalNode) eval.clone();
          } catch (CloneNotSupportedException e) {
            throw new TajoInternalError(e);
          }
          EvalTreeUtil.changeColumnRef(copy, column.getQualifiedName(),
              scanNode.getCanonicalName() + "." + column.getSimpleName());
          matched.add(copy);
          partitionEvals.add(eval);
        }
      }
    }

    context.pushingDownFilters.removeAll(partitionEvals);

    List<EvalNode> notMatched = TUtil.newList();

    // transform
    Map<EvalNode, EvalNode> transformed =
        findCanPushdownAndTransform(context, block, scanNode, null, notMatched, partitionColumns, 0);

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

    block.addAccessPath(scanNode, new SeqScanInfo(table));
    if (qual != null) { // if a matched qual exists
      scanNode.setQual(qual);

      // Index path can be identified only after filters are pushed into each scan.
      if(context.rewriteRuleContext.getQueryContext().getBool(SessionVars.INDEX_ENABLED)) {
        String databaseName, tableName;
        databaseName = CatalogUtil.extractQualifier(table.getName());
        tableName = CatalogUtil.extractSimpleName(table.getName());
        Set<Predicate> predicates = new HashSet<>();
        for (EvalNode eval : PlannerUtil.getAllEqualEvals(qual)) {
          BinaryEval binaryEval = (BinaryEval) eval;
          // TODO: consider more complex predicates
          if (binaryEval.getLeftExpr().getType() == EvalType.FIELD &&
              binaryEval.getRightExpr().getType() == EvalType.CONST) {
            predicates.add(new Predicate(binaryEval.getType(),
                ((FieldEval) binaryEval.getLeftExpr()).getColumnRef(),
                ((ConstEval) binaryEval.getRightExpr()).getValue()));
          } else if (binaryEval.getLeftExpr().getType() == EvalType.CONST &&
              binaryEval.getRightExpr().getType() == EvalType.FIELD) {
            predicates.add(new Predicate(binaryEval.getType(),
                ((FieldEval) binaryEval.getRightExpr()).getColumnRef(),
                ((ConstEval) binaryEval.getLeftExpr()).getValue()));
          }
        }

        // for every subset of the set of columns, find all matched index paths
        for (Set<Predicate> subset : Sets.powerSet(predicates)) {
          if (subset.size() == 0)
            continue;
          Column[] columns = extractColumns(subset);
          if (catalog.existIndexByColumns(databaseName, tableName, columns)) {
            IndexDesc indexDesc = catalog.getIndexByColumns(databaseName, tableName, columns);
            block.addAccessPath(scanNode, new IndexScanInfo(
                table.getStats(), indexDesc, getSimplePredicates(indexDesc, subset)));
          }
        }
      }
    }

    for (EvalNode matchedEval: matched) {
      transformed.remove(matchedEval);
    }

    context.setToOrigin(transformed);
    context.addFiltersTobePushed(notMatched);

    return scanNode;
  }

  private static class Predicate {
    Column column;
    Datum value;
    EvalType evalType;

    public Predicate(EvalType evalType, Column column, Datum value) {
      this.evalType = evalType;
      this.column = column;
      this.value = value;
    }
  }

  private static SimplePredicate[] getSimplePredicates(IndexDesc desc, Set<Predicate> predicates) {
    SimplePredicate[] simplePredicates = new SimplePredicate[predicates.size()];
    Map<Column, Datum> colToValue = new HashMap<>();
    for (Predicate predicate : predicates) {
      colToValue.put(predicate.column, predicate.value);
    }
    SortSpec [] keySortSpecs = desc.getKeySortSpecs();
    for (int i = 0; i < keySortSpecs.length; i++) {
      simplePredicates[i] = new SimplePredicate(keySortSpecs[i],
          colToValue.get(keySortSpecs[i].getSortKey()));
    }
    return simplePredicates;
  }

  private static Column[] extractColumns(Set<Predicate> predicates) {
    Column[] columns = new Column[predicates.size()];
    int i = 0;
    for (Predicate p : predicates) {
      columns[i++] = p.column;
    }
    return columns;
  }

  private void errorFilterPushDown(LogicalPlan plan, LogicalNode node,
                                   FilterPushDownContext context) {
    String prefix = "";
    StringBuilder notMatchedNodeStrBuilder = new StringBuilder();
    for (EvalNode notMatchedNode: context.pushingDownFilters) {
      notMatchedNodeStrBuilder.append(prefix).append(notMatchedNode.toString());
      if (prefix.isEmpty()) {
        prefix = ", ";
      }
    }
    throw new TajoInternalError("FilterPushDown failed cause some filters not matched: "
        + notMatchedNodeStrBuilder.toString() + "\n" +
        "Error node: " + node.getPlanString() + "\n" + plan.toString());
  }
}
