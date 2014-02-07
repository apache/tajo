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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.utils.SchemaUtil;
import org.apache.tajo.util.TUtil;

import java.util.*;

public class ProjectionPushDownRule extends
    BasicLogicalPlanVisitor<ProjectionPushDownRule.Context, LogicalNode> implements RewriteRule {
  /** Class Logger */
  private final Log LOG = LogFactory.getLog(ProjectionPushDownRule.class);
  private static final String name = "ProjectionPushDown";

  @Override
  public String getName() {
    return name;
  }

  @Override
  public boolean isEligible(LogicalPlan plan) {
    LogicalNode toBeOptimized = plan.getRootBlock().getRoot();

    if (PlannerUtil.checkIfDDLPlan(toBeOptimized) || !plan.getRootBlock().hasTableExpression()) {
      LOG.info("This query skips the logical optimization step.");
      return false;
    }

    return true;
  }

  @Override
  public LogicalPlan rewrite(LogicalPlan plan) throws PlanningException {
    LogicalPlan.QueryBlock rootBlock = plan.getRootBlock();

    LogicalPlan.QueryBlock topmostBlock = rootBlock;

    Stack<LogicalNode> stack = new Stack<LogicalNode>();
    Context context = new Context(plan);
    visit(context, plan, topmostBlock, topmostBlock.getRoot(), stack);

    return plan;
  }

  public static class TargetListManager {
    private LinkedHashMap<String, EvalNode> nameToEvalMap;
    private LinkedHashMap<EvalNode, String> evalToNameMap;
    private LinkedHashMap<String, Boolean> resolvedFlags;
    private LogicalPlan plan;

    public TargetListManager(LogicalPlan plan) {
      this.plan = plan;
      nameToEvalMap = new LinkedHashMap<String, EvalNode>();
      evalToNameMap = new LinkedHashMap<EvalNode, String>();
      resolvedFlags = new LinkedHashMap<String, Boolean>();
    }

    public TargetListManager(TargetListManager targetListMgr) {
      this.plan = targetListMgr.plan;
      nameToEvalMap = new LinkedHashMap<String, EvalNode>(targetListMgr.nameToEvalMap);
      evalToNameMap = new LinkedHashMap<EvalNode, String>(targetListMgr.evalToNameMap);
      resolvedFlags = new LinkedHashMap<String, Boolean>(targetListMgr.resolvedFlags);
    }

    private String add(String name, EvalNode evalNode) throws PlanningException {
      if (evalNode.getType() == EvalType.CONST) {
        nameToEvalMap.put(name, evalNode);
        resolvedFlags.put(name, false);
        return name;
      }
      if (evalToNameMap.containsKey(evalNode)) {
        name = evalToNameMap.get(evalNode);
      } else {

        // Name can be conflicts between a column reference and an aliased EvalNode.
        // Example, a SQL statement 'select l_orderkey + l_partkey as total ...' leads to
        // two EvalNodes: a column reference total and an EvalNode (+, l_orderkey, l_partkey)
        // If they are inserted into here, their names are conflict to each other, and one of them is removed.
        // In this case, we just keep an original eval node instead of a column reference.
        // This is because a column reference that points to an aliased EvalNode can be restored from the given alias.
        if (nameToEvalMap.containsKey(name)) {
          EvalNode storedEvalNode = nameToEvalMap.get(name);
          if (!storedEvalNode.equals(evalNode)) {
            if (storedEvalNode.getType() != EvalType.FIELD && evalNode.getType() != EvalType.FIELD) {
              throw new PlanningException("Duplicate alias: " + evalNode);
            }
            if (storedEvalNode.getType() == EvalType.FIELD) {
              nameToEvalMap.put(name, evalNode);
            }
          }
        } else {
          nameToEvalMap.put(name, evalNode);
        }

        evalToNameMap.put(evalNode, name);
        resolvedFlags.put(name, false);

        for (Column column : EvalTreeUtil.findDistinctRefColumns(evalNode)) {
          add(new FieldEval(column));
        }
      }
      return name;
    }

    public Collection<String> getNames() {
      return nameToEvalMap.keySet();
    }

    public String add(Target target) throws PlanningException {
      return add(target.getCanonicalName(), target.getEvalTree());
    }

    public String add(EvalNode evalNode) throws PlanningException {
      String name;
      if (evalToNameMap.containsKey(evalNode)) {
        name = evalToNameMap.get(evalNode);
      } else {
        if (evalNode.getType() == EvalType.FIELD) {
          FieldEval fieldEval = (FieldEval) evalNode;
          name = fieldEval.getName();
        } else {
          name = plan.newGeneratedFieldName(evalNode);
        }
        add(name, evalNode);
      }
      return name;
    }

    public Target getTarget(String name) {
      if (!nameToEvalMap.containsKey(name)) {
        throw new RuntimeException("No Such target name: " + name);
      }
      EvalNode evalNode = nameToEvalMap.get(name);
      if (evalNode.getType() == EvalType.FIELD && evalNode.getName().equals(name)) {
        return new Target((FieldEval)evalNode);
      } else {
        return new Target(evalNode, name);
      }
    }

    public boolean isResolved(String name) {
      if (!nameToEvalMap.containsKey(name)) {
        throw new RuntimeException("No Such target name: " + name);
      }

      return resolvedFlags.get(name);
    }

    public void resolve(Target target) {
      EvalNode evalNode = target.getEvalTree();
      if (evalNode.getType() == EvalType.CONST) { // if constant value
        resolvedFlags.put(name, true);
        return; // keep it raw always
      }
      if (!evalToNameMap.containsKey(evalNode)) {
        throw new RuntimeException("No such eval: " + evalNode);
      }
      String name = evalToNameMap.get(evalNode);
      resolvedFlags.put(name, true);
    }

    public Iterator<Target> getFilteredTargets(Set<String> required) {
      return new FilteredTargetIterator(required);
    }

    class FilteredTargetIterator implements Iterator<Target> {
      List<Target> filtered = TUtil.newList();

      public FilteredTargetIterator(Set<String> required) {
        for (Map.Entry<String,EvalNode> entry : nameToEvalMap.entrySet()) {
          if (required.contains(entry.getKey())) {
            filtered.add(getTarget(entry.getKey()));
          }
        }
      }

      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public Target next() {
        return null;
      }

      @Override
      public void remove() {
      }
    }

    public String toString() {
      int resolved = 0;
      for (Boolean flag: resolvedFlags.values()) {
        if (flag) {
          resolved++;
        }
      }
      return "eval=" + resolvedFlags.size() + ", resolved=" + resolved;
    }
  }

  static class Context {
    TargetListManager targetListMgr;
    Set<String> requiredSet;

    public Context(LogicalPlan plan) {
      requiredSet = new LinkedHashSet<String>();
      targetListMgr = new TargetListManager(plan);
    }

    public Context(LogicalPlan plan, Collection<String> requiredSet) {
      this.requiredSet = new LinkedHashSet<String>(requiredSet);
      targetListMgr = new TargetListManager(plan);
    }

    public Context(Context upperContext) {
      this.requiredSet = new LinkedHashSet<String>(upperContext.requiredSet);
      targetListMgr = upperContext.targetListMgr;
    }

    public String addExpr(Target target) throws PlanningException {
      String reference = targetListMgr.add(target);
      addNecessaryReferences(target.getEvalTree());
      return reference;
    }

    public String addExpr(EvalNode evalNode) throws PlanningException {
      String reference = targetListMgr.add(evalNode);
      addNecessaryReferences(evalNode);
      return reference;
    }

    private void addNecessaryReferences(EvalNode evalNode) {
      for (Column column : EvalTreeUtil.findDistinctRefColumns(evalNode)) {
        requiredSet.add(column.getQualifiedName());
      }
    }

    @Override
    public String toString() {
      return "required=" + requiredSet.size() + "," + targetListMgr.toString();
    }
  }

  @Override
  public LogicalNode visitRoot(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalRootNode node,
                          Stack<LogicalNode> stack) throws PlanningException {
    LogicalNode child = super.visitRoot(context, plan, block, node, stack);
    node.setInSchema(child.getOutSchema());
    node.setOutSchema(child.getOutSchema());
    return node;
  }

  @Override
  public LogicalNode visitProjection(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                     ProjectionNode node, Stack<LogicalNode> stack) throws PlanningException {
    Context newContext = new Context(context);
    String [] referenceNames = new String[node.getTargets().length];
    for (int i = 0; i < node.getTargets().length; i++) {
      referenceNames[i] = newContext.addExpr(node.getTargets()[i]);
    }

    LogicalNode child = super.visitProjection(newContext, plan, block, node, stack);

    int resolvingCount = 0;
    List<Target> finalTargets = TUtil.newList();
    for (String referenceName : referenceNames) {
      Target target = context.targetListMgr.getTarget(referenceName);

      if (context.targetListMgr.isResolved(referenceName)) {
        finalTargets.add(new Target(new FieldEval(target.getNamedColumn())));
      } else if (LogicalPlanner.checkIfBeEvaluatedAtThis(target.getEvalTree(), node)) {
        finalTargets.add(target);
        context.targetListMgr.resolve(target);
        resolvingCount++;
      }
    }

    // Removing ProjectionNode
    // TODO - Consider INSERT and CTAS statement, and then remove the check of stack.empty.
    if (resolvingCount == 0 && PlannerUtil.targetToSchema(finalTargets).equals(child.getOutSchema())) {
      if (stack.empty()) {
        // if it is topmost, set it as the root of this block.
        block.setRoot(child);
      } else {
        LogicalNode parentNode = stack.peek();
        switch (parentNode.getType()) {
        case ROOT:
          LogicalRootNode rootNode = (LogicalRootNode) parentNode;
          rootNode.setChild(child);
          rootNode.setInSchema(child.getOutSchema());
          rootNode.setOutSchema(child.getOutSchema());
          break;
        case TABLE_SUBQUERY:
          TableSubQueryNode tableSubQueryNode = (TableSubQueryNode) parentNode;
          tableSubQueryNode.setSubQuery(child);
          break;
        case STORE:
          StoreTableNode storeTableNode = (StoreTableNode) parentNode;
          storeTableNode.setChild(child);
          storeTableNode.setInSchema(child.getOutSchema());
          break;
        case INSERT:
          InsertNode insertNode = (InsertNode) parentNode;
          insertNode.setSubQuery(child);
          break;
        case CREATE_TABLE:
          CreateTableNode createTableNode = (CreateTableNode) parentNode;
          createTableNode.setChild(child);
          createTableNode.setInSchema(child.getOutSchema());
          break;
        default:
          throw new PlanningException("Unexpected Parent Node: " + parentNode.getType());
        }
        plan.addHistory("ProjectionNode is eliminated.");
      }

      return child;

    } else {
      node.setInSchema(child.getOutSchema());
      node.setTargets(finalTargets.toArray(new Target[finalTargets.size()]));
      return node;
    }
  }

  public LogicalNode visitLimit(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, LimitNode node,
                           Stack<LogicalNode> stack) throws PlanningException {
    LogicalNode child = super.visitLimit(context, plan, block, node, stack);

    node.setInSchema(child.getOutSchema());
    node.setOutSchema(child.getOutSchema());
    return node;
  }

  @Override
  public LogicalNode visitSort(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                               SortNode node, Stack<LogicalNode> stack) throws PlanningException {
    Context newContext = new Context(context);

    final int sortKeyNum = node.getSortKeys().length;
    String [] keyNames = new String[sortKeyNum];
    for (int i = 0; i < sortKeyNum; i++) {
      SortSpec sortSpec = node.getSortKeys()[i];
      keyNames[i] = newContext.addExpr(new FieldEval(sortSpec.getSortKey()));
    }

    LogicalNode child = super.visitSort(newContext, plan, block, node, stack);

    node.setInSchema(child.getOutSchema());
    node.setOutSchema(child.getOutSchema());
    return node;
  }

  @Override
  public LogicalNode visitHaving(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, HavingNode node,
                            Stack<LogicalNode> stack) throws PlanningException {
    Context newContext = new Context(context);
    String referenceName = newContext.targetListMgr.add(node.getQual());
    newContext.addNecessaryReferences(node.getQual());

    LogicalNode child = super.visitHaving(newContext, plan, block, node, stack);

    node.setInSchema(child.getOutSchema());
    node.setOutSchema(child.getOutSchema());

    Target target = context.targetListMgr.getTarget(referenceName);
    if (newContext.targetListMgr.isResolved(referenceName)) {
      node.setQual(new FieldEval(target.getNamedColumn()));
    } else {
      node.setQual(target.getEvalTree());
      newContext.targetListMgr.resolve(target);
    }

    return node;
  }

  public LogicalNode visitGroupBy(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, GroupbyNode node,
                             Stack<LogicalNode> stack) throws PlanningException {
    Context newContext = new Context(context);

    // Getting grouping key names
    final int groupingKeyNum = node.getGroupingColumns().length;
    String [] groupingKeyNames = null;
    if (groupingKeyNum > 0) {
      groupingKeyNames = new String[groupingKeyNum];
      for (int i = 0; i < groupingKeyNum; i++) {
        FieldEval fieldEval = new FieldEval(node.getGroupingColumns()[i]);
        groupingKeyNames[i] = newContext.addExpr(fieldEval);
      }
    }

    // Getting eval names

    final String [] aggEvalNames;
    if (node.hasAggFunctions()) {
      final int evalNum = node.getAggFunctions().length;
      aggEvalNames = new String[evalNum];
      for (int evalIdx = 0, targetIdx = groupingKeyNum; targetIdx < node.getTargets().length; evalIdx++, targetIdx++) {
        Target target = node.getTargets()[targetIdx];
        EvalNode evalNode = node.getAggFunctions()[evalIdx];
        aggEvalNames[evalIdx] = newContext.addExpr(new Target(evalNode, target.getCanonicalName()));
      }
    } else {
      aggEvalNames = null;
    }

    // visit a child node
    LogicalNode child = super.visitGroupBy(newContext, plan, block, node, stack);

    node.setInSchema(child.getOutSchema());

    if (groupingKeyNum > 0) {
      // Restoring grouping key columns
      final Column [] groupingColumns = new Column[groupingKeyNum];
      for (int i = 0; i < groupingKeyNum; i++) {
        String groupingKey = groupingKeyNames[i];

        Target target = context.targetListMgr.getTarget(groupingKey);
        if (context.targetListMgr.isResolved(groupingKey)) {
          groupingColumns[i] = target.getNamedColumn();
        } else {
          if (target.getEvalTree().getType() == EvalType.FIELD) {
            groupingColumns[i] = target.getNamedColumn();
          } else {
            throw new PlanningException("Cannot evaluate this expression in grouping keys: " + target.getEvalTree());
          }
        }
      }
      node.setGroupingColumns(groupingColumns);
    }

    // Getting projected targets
    if (node.hasAggFunctions()) {
      AggregationFunctionCallEval [] aggEvals = new AggregationFunctionCallEval[aggEvalNames.length];
      int i = 0;
      for (Iterator<String> it = getFilteredReferences(aggEvalNames, TUtil.newList(aggEvalNames)); it.hasNext();) {

        String referenceName = it.next();
        Target target = context.targetListMgr.getTarget(referenceName);

        if (LogicalPlanner.checkIfBeEvaluatedAtGroupBy(target.getEvalTree(), node)) {
          aggEvals[i++] = target.getEvalTree();
          context.targetListMgr.resolve(target);
        }
      }
      if (aggEvals.length > 0) {
        node.setAggFunctions(aggEvals);
      }
    }
    Target [] targets = buildGroupByTarget(node, aggEvalNames);
    node.setTargets(targets);

    return node;
  }

  public static Target [] buildGroupByTarget(GroupbyNode groupbyNode, String [] aggEvalNames) {
    final int groupingKeyNum = groupbyNode.getGroupingColumns().length;
    final int aggrFuncNum = aggEvalNames != null ? aggEvalNames.length : 0;
    EvalNode [] aggEvalNodes = groupbyNode.getAggFunctions();
    Target [] targets = new Target[groupingKeyNum + aggrFuncNum];

    for (int groupingKeyIdx = 0; groupingKeyIdx < groupingKeyNum; groupingKeyIdx++) {
      targets[groupingKeyIdx] = new Target(new FieldEval(groupbyNode.getGroupingColumns()[groupingKeyIdx]));
    }

    if (aggEvalNames != null) {
      for (int aggrFuncIdx = 0, targetIdx = groupingKeyNum; aggrFuncIdx < aggrFuncNum; aggrFuncIdx++, targetIdx++) {
        targets[targetIdx] =
            new Target(new FieldEval(aggEvalNames[aggrFuncIdx], aggEvalNodes[aggrFuncIdx].getValueType()));
      }
    }

    return targets;
  }

  public LogicalNode visitFilter(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 SelectionNode node, Stack<LogicalNode> stack) throws PlanningException {
    Context newContext = new Context(context);
    String referenceName = newContext.targetListMgr.add(node.getQual());
    newContext.addNecessaryReferences(node.getQual());

    LogicalNode child = super.visitFilter(newContext, plan, block, node, stack);

    node.setInSchema(child.getOutSchema());
    node.setOutSchema(child.getOutSchema());

    Target target = context.targetListMgr.getTarget(referenceName);
    if (newContext.targetListMgr.isResolved(referenceName)) {
      node.setQual(new FieldEval(target.getNamedColumn()));
    } else {
      node.setQual(target.getEvalTree());
      newContext.targetListMgr.resolve(target);
    }

    return node;
  }

  public LogicalNode visitJoin(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, JoinNode node,
                          Stack<LogicalNode> stack) throws PlanningException {
    Context newContext = new Context(context);

    String joinQualReference = null;
    if (node.hasJoinQual()) {
      joinQualReference = newContext.addExpr(node.getJoinQual());
      newContext.addNecessaryReferences(node.getJoinQual());
    }

    String [] referenceNames = null;
    if (node.hasTargets()) {
      referenceNames = new String[node.getTargets().length];
      int i = 0;
      for (Iterator<Target> it = getFilteredTarget(node.getTargets(), context.requiredSet); it.hasNext();) {
        Target target = it.next();
        referenceNames[i++] = newContext.addExpr(target);
      }
    }

    stack.push(node);
    LogicalNode left = visit(newContext, plan, block, node.getLeftChild(), stack);
    LogicalNode right = visit(newContext, plan, block, node.getRightChild(), stack);
    stack.pop();

    Schema merged = SchemaUtil.merge(left.getOutSchema(), right.getOutSchema());

    node.setInSchema(merged);

    if (node.hasJoinQual()) {
      Target target = context.targetListMgr.getTarget(joinQualReference);
      if (newContext.targetListMgr.isResolved(joinQualReference)) {
        throw new PlanningException("Join condition must be evaluated in the proper Join Node: " + joinQualReference);
      } else {
        node.setJoinQual(target.getEvalTree());
        newContext.targetListMgr.resolve(target);
      }
    }

    List<Target> projectedTargets = TUtil.newList();
    for (Iterator<String> it = getFilteredReferences(context.targetListMgr.getNames(),
        context.requiredSet); it.hasNext();) {
      String referenceName = it.next();
      Target target = context.targetListMgr.getTarget(referenceName);

      if (context.targetListMgr.isResolved(referenceName)) {
        Target fieldReference = new Target(new FieldEval(target.getNamedColumn()));
        if (LogicalPlanner.checkIfBeEvaluatedAtJoin(block, fieldReference.getEvalTree(), node,
            stack.peek().getType() != NodeType.JOIN)) {
          projectedTargets.add(fieldReference);
        }
      } else if (LogicalPlanner.checkIfBeEvaluatedAtJoin(block, target.getEvalTree(), node,
          stack.peek().getType() != NodeType.JOIN)) {
        projectedTargets.add(target);
        context.targetListMgr.resolve(target);
      }
    }

    node.setTargets(projectedTargets.toArray(new Target[projectedTargets.size()]));

    return node;
  }

  static Iterator<String> getFilteredReferences(Collection<String> targetNames, Set<String> required) {
    return new FilteredStringsIterator(targetNames, required);
  }

  static Iterator<String> getFilteredReferences(String [] targetNames, Collection<String> required) {
    return new FilteredStringsIterator(targetNames, required);
  }

  static class FilteredStringsIterator implements Iterator<String> {
    Iterator<String> iterator;

    FilteredStringsIterator(Collection<String> targetNames, Collection<String> required) {
      List<String> filtered = TUtil.newList();
      for (String name : targetNames) {
        if (required.contains(name)) {
          filtered.add(name);
        }
      }

      iterator = filtered.iterator();
    }

    FilteredStringsIterator(String [] targetNames, Collection<String> required) {
      this(TUtil.newList(targetNames), required);
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public String next() {
      return iterator.next();
    }

    @Override
    public void remove() {
    }
  }

  static Iterator<Target> getFilteredTarget(Target[] targets, Set<String> required) {
    return new FilteredIterator(targets, required);
  }

  static class FilteredIterator implements Iterator<Target> {
    Iterator<Target> iterator;

    FilteredIterator(Target [] targets, Set<String> requiredReferences) {
      List<Target> filtered = TUtil.newList();
      Map<String, Target> targetSet = new HashMap<String, Target>();
      for (Target t : targets) {
        // Only should keep an raw target instead of field reference.
        if (targetSet.containsKey(t.getCanonicalName())) {
          Target targetInSet = targetSet.get(t.getCanonicalName());
          EvalNode evalNode = targetInSet.getEvalTree();
          if (evalNode.getType() == EvalType.FIELD && t.getEvalTree().getType() != EvalType.FIELD) {
            targetSet.put(t.getCanonicalName(), t);
          }
        } else {
          targetSet.put(t.getCanonicalName(), t);
        }
      }

      for (String name : requiredReferences) {
        if (targetSet.containsKey(name)) {
          filtered.add(targetSet.get(name));
        }
      }

      iterator = filtered.iterator();
    }
    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Target next() {
      return iterator.next();
    }

    @Override
    public void remove() {
    }
  }

  @Override
  public LogicalNode visitUnion(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, UnionNode node,
                           Stack<LogicalNode> stack) throws PlanningException {

    LogicalPlan.QueryBlock leftBlock = plan.getBlock(node.getLeftChild());
    LogicalPlan.QueryBlock rightBlock = plan.getBlock(node.getRightChild());

    Context leftContext = new Context(plan, PlannerUtil.toQualifiedFieldNames(context.requiredSet,
        leftBlock.getName()));
    Context rightContext = new Context(plan, PlannerUtil.toQualifiedFieldNames(context.requiredSet,
        rightBlock.getName()));

    stack.push(node);
    visit(leftContext, plan, leftBlock, leftBlock.getRoot(), new Stack<LogicalNode>());
    visit(rightContext, plan, rightBlock, rightBlock.getRoot(), new Stack<LogicalNode>());
    stack.pop();
    return node;
  }

  public LogicalNode visitScan(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, ScanNode node,
                          Stack<LogicalNode> stack) throws PlanningException {

    Context newContext = new Context(context);

    Target [] targets;
    if (node.hasTargets()) {
      targets = node.getTargets();
    } else {
      targets = PlannerUtil.schemaToTargets(node.getTableSchema());
    }

    List<Target> projectedTargets = TUtil.newList();
    for (Iterator<Target> it = getFilteredTarget(targets, newContext.requiredSet); it.hasNext();) {
      Target target = it.next();
      newContext.addExpr(target);
    }

    for (Iterator<Target> it = getFilteredTarget(targets, context.requiredSet); it.hasNext();) {
      Target target = it.next();

      if (LogicalPlanner.checkIfBeEvaluatedAtRelation(block, target.getEvalTree(), node)) {
        projectedTargets.add(target);
        newContext.targetListMgr.resolve(target);
      }
    }

    node.setTargets(projectedTargets.toArray(new Target[projectedTargets.size()]));
    return node;
  }

  @Override
  public LogicalNode visitPartitionedTableScan(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                               PartitionedTableScanNode node, Stack<LogicalNode> stack)
      throws PlanningException {

    Context newContext = new Context(context);

    Target [] targets;
    if (node.hasTargets()) {
      targets = node.getTargets();
    } else {
      targets = PlannerUtil.schemaToTargets(node.getOutSchema());
    }

    List<Target> projectedTargets = TUtil.newList();
    for (Iterator<Target> it = getFilteredTarget(targets, newContext.requiredSet); it.hasNext();) {
      Target target = it.next();
      newContext.addExpr(target);
    }

    for (Iterator<Target> it = getFilteredTarget(targets, context.requiredSet); it.hasNext();) {
      Target target = it.next();

      if (LogicalPlanner.checkIfBeEvaluatedAtRelation(block, target.getEvalTree(), node)) {
        projectedTargets.add(target);
        newContext.targetListMgr.resolve(target);
      }
    }

    node.setTargets(projectedTargets.toArray(new Target[projectedTargets.size()]));
    return node;
  }

  @Override
  public LogicalNode visitTableSubQuery(Context upperContext, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                   TableSubQueryNode node, Stack<LogicalNode> stack) throws PlanningException {
    Context childContext = new Context(plan, upperContext.requiredSet);
    stack.push(node);
    LogicalNode child = super.visitTableSubQuery(childContext, plan, block, node, stack);
    node.setSubQuery(child);
    stack.pop();

    Context newContext = new Context(upperContext);

    Target [] targets;
    if (node.hasTargets()) {
      targets = node.getTargets();
    } else {
      targets = PlannerUtil.schemaToTargets(node.getOutSchema());
    }

    List<Target> projectedTargets = TUtil.newList();
    for (Iterator<Target> it = getFilteredTarget(targets, newContext.requiredSet); it.hasNext();) {
      Target target = it.next();
      childContext.addExpr(target);
    }

    for (Iterator<Target> it = getFilteredTarget(targets, upperContext.requiredSet); it.hasNext();) {
      Target target = it.next();

      if (LogicalPlanner.checkIfBeEvaluatedAtRelation(block, target.getEvalTree(), node)) {
        projectedTargets.add(target);
        childContext.targetListMgr.resolve(target);
      }
    }

    node.setTargets(projectedTargets.toArray(new Target[projectedTargets.size()]));
    LogicalPlanner.verifyProjectedFields(block, node);
    return node;
  }

  @Override
  public LogicalNode visitInsert(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, InsertNode node,
                            Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    visit(context, plan, block, node.getChild(), stack);
    stack.pop();
    return node;
  }
}
