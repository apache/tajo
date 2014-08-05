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

import com.google.common.collect.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.LogicalPlan.QueryBlock;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.utils.SchemaUtil;
import org.apache.tajo.util.TUtil;

import java.util.*;

/**
 * ProjectionPushDownRule deploys expressions in a selection list to proper
 * {@link org.apache.tajo.engine.planner.logical.Projectable}
 * nodes. In this process, the expressions are usually pushed down into as lower as possible.
 * It also enables scanners to read only necessary columns.
 */
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

    if (PlannerUtil.checkIfDDLPlan(toBeOptimized)) {
      return false;
    }
    for (QueryBlock eachBlock: plan.getQueryBlocks()) {
      if (eachBlock.hasTableExpression()) {
        return true;
      }
    }
    return false;
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

  /**
   * <h2>What is TargetListManager?</h2>
   * It manages all expressions used in a query block, and their reference names.
   * TargetListManager provides a way to find an expression by a reference name.
   * It keeps a set of expressions, and one or more reference names can point to
   * the same expression.
   *
   * Also, TargetListManager keeps the evaluation state of each expression.
   * The evaluation state is a boolean state to indicate whether the expression
   * was evaluated in descendant node or not. If an expression is evaluated,
   * the evaluation state is changed to TRUE. It also means that
   * the expression can be referred by an column reference instead of evaluating the expression.
   *
   * Consider an example query:
   *
   * SELECT sum(l_orderkey + 1) from lineitem where l_partkey > 1;
   *
   * In this case, an expression sum(l_orderkey + 1) is divided into two sub expressions:
   * <ul>
   *  <li>$1 <- l_orderkey + 1</li>
   *  <li>$2 <- sum($1)</li>
   * </ul>
   *
   * <code>$1</code> is a simple arithmetic operation, and $2 is an aggregation function.
   * <code>$1</code> is evaluated in ScanNode because it's just a simple arithmetic operation.
   * So, the evaluation state of l_orderkey + 1 initially
   * is false, but the state will be true after ScanNode.
   *
   * In contrast, sum($1) is evaluated at GroupbyNode. So, its evaluation state is changed
   * after GroupByNode.
   *
   * <h2>Why is TargetListManager necessary?</h2>
   *
   * Expressions used in a query block can be divided into various categories according to
   * the possible {@link Projectable} nodes. Their references become available depending on
   * the Projectable node at which expressions are evaluated. It manages the expressions and
   * references for optimized places of expressions. It performs duplicated removal and enables
   * common expressions to be shared with two or more Projectable nodes. It also helps Projectable
   * nodes to find correct column references.
   */
  public static class TargetListManager {
    private Integer seqId = 0;

    /**
     * Why should we use LinkedHashMap for those maps ?
     *
     * These maps are mainly by the target list of each projectable node
     * (i.e., ProjectionNode, GroupbyNode, JoinNode, and ScanNode).
     * The projection node removal occurs only when the projection node's output
     * schema and its child's output schema are equivalent to each other.
     *
     * If we keep the inserted order of all expressions. It would make the possibility
     * of projection node removal higher.
     **/

    /** A Map: Name -> Id */
    private LinkedHashMap<String, Integer> nameToIdBiMap;
    /** Map: Id <-> EvalNode */
    private BiMap<Integer, EvalNode> idToEvalBiMap;
    /** Map: Id -> Names */
    private LinkedHashMap<Integer, List<String>> idToNamesMap;
    /** Map: Id -> Boolean */
    private LinkedHashMap<Integer, Boolean> evaluationStateMap;
    /** Map: alias name -> Id */
    private LinkedHashMap<String, Integer> aliasMap;

    private LogicalPlan plan;

    public TargetListManager(LogicalPlan plan) {
      this.plan = plan;
      nameToIdBiMap = Maps.newLinkedHashMap();
      idToEvalBiMap = HashBiMap.create();
      idToNamesMap = Maps.newLinkedHashMap();
      evaluationStateMap = Maps.newLinkedHashMap();
      aliasMap = Maps.newLinkedHashMap();
    }

    private int getNextSeqId() {
      return seqId++;
    }

    /**
     * If some expression is duplicated, we call an alias indicating the duplicated expression 'native alias'.
     * This method checks whether a reference is native alias or not.
     *
     * @param name The reference name
     * @return True if the reference is native alias. Otherwise, it will return False.
     */
    public boolean isNativeAlias(String name) {
      return aliasMap.containsKey(name);
    }

    /**
     * This method retrieves the name indicating actual expression that an given alias indicate.
     *
     * @param name an alias name
     * @return Real reference name
     */
    public String getRealReferenceName(String name) {
      int refId = aliasMap.get(name);
      return getPrimaryName(refId);
    }

    /**
     * Add an expression with a specified name, which is usually an alias.
     * Later, you can refer this expression by the specified name.
     */
    private String add(String specifiedName, EvalNode evalNode) throws PlanningException {

      // if a name already exists, it only just keeps an actual
      // expression instead of a column reference.
      if (nameToIdBiMap.containsKey(specifiedName)) {

        int refId = nameToIdBiMap.get(specifiedName);
        EvalNode found = idToEvalBiMap.get(refId);
        if (found != null) {
          if (evalNode.equals(found)) { // if input expression already exists
            return specifiedName;
          } else {
            // The case where if existing reference name and a given reference name are the same to each other and
            // existing EvalNode and a given EvalNode is the different
            if (found.getType() != EvalType.FIELD && evalNode.getType() != EvalType.FIELD) {
              throw new PlanningException("Duplicate alias: " + evalNode);
            }

            if (found.getType() == EvalType.FIELD) {
              Integer daggling = idToEvalBiMap.inverse().get(evalNode);
              idToEvalBiMap.forcePut(refId, evalNode);
              if (daggling != null) {
                String name = getPrimaryName(daggling);
                idToNamesMap.remove(daggling);
                nameToIdBiMap.put(name, refId);
                if (!idToNamesMap.get(refId).contains(name)) {
                  TUtil.putToNestedList(idToNamesMap, refId, name);
                }
              }
            }
          }
        }
      }

      int refId;
      if (idToEvalBiMap.inverse().containsKey(evalNode)) {
        refId = idToEvalBiMap.inverse().get(evalNode);
        aliasMap.put(specifiedName, refId);

      } else {
        refId = getNextSeqId();
        idToEvalBiMap.put(refId, evalNode);
        TUtil.putToNestedList(idToNamesMap, refId, specifiedName);
        for (Column column : EvalTreeUtil.findUniqueColumns(evalNode)) {
          add(new FieldEval(column));
        }
        evaluationStateMap.put(refId, false);
      }

      nameToIdBiMap.put(specifiedName, refId);

      return specifiedName;
    }

    /**
     * Adds an expression without any name. It returns an automatically
     * generated name. It can be also used for referring this expression.
     */
    public String add(EvalNode evalNode) throws PlanningException {
      String name;

      if (evalNode.getType() == EvalType.FIELD) {
        FieldEval fieldEval = (FieldEval) evalNode;
        if (nameToIdBiMap.containsKey(fieldEval.getName())) {
          int refId = nameToIdBiMap.get(fieldEval.getName());
          return getPrimaryName(refId);
        }
      }

      if (idToEvalBiMap.inverse().containsKey(evalNode)) {
        int refId = idToEvalBiMap.inverse().get(evalNode);
        return getPrimaryName(refId);
      }

      if (evalNode.getType() == EvalType.FIELD) {
        FieldEval fieldEval = (FieldEval) evalNode;
        name = fieldEval.getName();
      } else {
        name = plan.generateUniqueColumnName(evalNode);
      }

      return add(name, evalNode);
    }

    public Collection<String> getNames() {
      return nameToIdBiMap.keySet();
    }

    public String add(Target target) throws PlanningException {
      return add(target.getCanonicalName(), target.getEvalTree());
    }

    /**
     * Each expression can have one or more names.
     * We call a name added with an expression firstly as the primary name.
     * It has a special meaning. Since duplicated expression in logical planning are removed,
     * the primary name is only used for each expression during logical planning.
     *
     * @param refId The identifier of an expression
     * @param name The name to check if it is the primary name.
     * @return True if this name is the primary added name. Otherwise, False.
     */
    private boolean isPrimaryName(int refId, String name) {
      if (idToNamesMap.get(refId).size() > 0) {
        return getPrimaryName(refId).equals(name);
      } else {
        return false;
      }
    }

    private String getPrimaryName(int refId) {
      return idToNamesMap.get(refId).get(0);
    }

    public Target getTarget(String name) {
      if (!nameToIdBiMap.containsKey(name)) {
        throw new RuntimeException("No Such target name: " + name);
      }
      int id = nameToIdBiMap.get(name);
      EvalNode evalNode = idToEvalBiMap.get(id);

      // if it is a constant value, just returns a constant because it can be evaluated everywhere.
      if (evalNode.getType() == EvalType.CONST) {
        return new Target(evalNode, name);
      }

      // if a name is not the primary name, it means that its expression may be already evaluated and
      // can just refer a value. Consider an example as follows:
      //
      // select l_orderkey + 1 as total1, l_orderkey + 1 as total2 from lineitem
      //
      // In this case, total2 will meet the following condition. Then, total2 can
      // just refer the result of total1 rather than calculating l_orderkey + 1.
      if (!isPrimaryName(id, name) && isEvaluated(getPrimaryName(id))) {
        evalNode = new FieldEval(getPrimaryName(id), evalNode.getValueType());
      }

      // if it is a column reference itself, just returns a column reference without any alias.
      if (evalNode.getType() == EvalType.FIELD && evalNode.getName().equals(name)) {
        return new Target((FieldEval)evalNode);
      } else { // otherwise, it returns an expression.
        return new Target(evalNode, name);
      }
    }

    public boolean isEvaluated(String name) {
      if (!nameToIdBiMap.containsKey(name)) {
        throw new RuntimeException("No Such target name: " + name);
      }
      int refId = nameToIdBiMap.get(name);
      return evaluationStateMap.get(refId);
    }

    public void markAsEvaluated(Target target) {
      int refId = nameToIdBiMap.get(target.getCanonicalName());
      EvalNode evalNode = target.getEvalTree();
      if (!idToNamesMap.containsKey(refId)) {
        throw new RuntimeException("No such eval: " + evalNode);
      }
      evaluationStateMap.put(refId, true);
    }

    public Iterator<Target> getFilteredTargets(Set<String> required) {
      return new FilteredTargetIterator(required);
    }

    class FilteredTargetIterator implements Iterator<Target> {
      List<Target> filtered = TUtil.newList();
      Iterator<Target> iterator;

      public FilteredTargetIterator(Set<String> required) {
        for (String name : nameToIdBiMap.keySet()) {
          if (required.contains(name)) {
            filtered.add(getTarget(name));
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

    public String toString() {
      int evaluated = 0;
      for (Boolean flag: evaluationStateMap.values()) {
        if (flag) {
          evaluated++;
        }
      }
      return "eval=" + evaluationStateMap.size() + ", evaluated=" + evaluated;
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
      for (Column column : EvalTreeUtil.findUniqueColumns(evalNode)) {
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
    Target [] targets = node.getTargets();
    int targetNum = targets.length;
    String [] referenceNames = new String[targetNum];
    for (int i = 0; i < targetNum; i++) {
      referenceNames[i] = newContext.addExpr(targets[i]);
    }

    LogicalNode child = super.visitProjection(newContext, plan, block, node, stack);

    node.setInSchema(child.getOutSchema());

    int evaluationCount = 0;
    List<Target> finalTargets = TUtil.newList();
    for (String referenceName : referenceNames) {
      Target target = context.targetListMgr.getTarget(referenceName);

      if (target.getEvalTree().getType() == EvalType.CONST) {
        finalTargets.add(target);
      } else if (context.targetListMgr.isEvaluated(referenceName)) {
        if (context.targetListMgr.isNativeAlias(referenceName)) {
          String realRefName = context.targetListMgr.getRealReferenceName(referenceName);
          finalTargets.add(new Target(new FieldEval(realRefName, target.getDataType()), referenceName));
        } else {
          finalTargets.add(new Target(new FieldEval(target.getNamedColumn())));
        }
      } else if (LogicalPlanner.checkIfBeEvaluatedAtThis(target.getEvalTree(), node)) {
        finalTargets.add(target);
        context.targetListMgr.markAsEvaluated(target);
        evaluationCount++;
      }
    }

    node.setTargets(finalTargets.toArray(new Target[finalTargets.size()]));
    LogicalPlanner.verifyProjectedFields(block, node);

    // Removing ProjectionNode
    // TODO - Consider INSERT and CTAS statement, and then remove the check of stack.empty.
    if (evaluationCount == 0 && PlannerUtil.targetToSchema(finalTargets).equals(child.getOutSchema())) {
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

    // it rewrite sortkeys. This rewrite sets right column names and eliminates duplicated sort keys.
    List<SortSpec> sortSpecs = new ArrayList<SortSpec>();
    for (int i = 0; i < keyNames.length; i++) {
      String sortKey = keyNames[i];
      Target target = context.targetListMgr.getTarget(sortKey);
      if (context.targetListMgr.isEvaluated(sortKey)) {
        Column c = target.getNamedColumn();
        SortSpec sortSpec = new SortSpec(c, node.getSortKeys()[i].isAscending(), node.getSortKeys()[i].isNullFirst());
        if (!sortSpecs.contains(sortSpec)) {
          sortSpecs.add(sortSpec);
        }
      } else {
        if (target.getEvalTree().getType() == EvalType.FIELD) {
          Column c = ((FieldEval)target.getEvalTree()).getColumnRef();
          SortSpec sortSpec = new SortSpec(c, node.getSortKeys()[i].isAscending(), node.getSortKeys()[i].isNullFirst());
          if (!sortSpecs.contains(sortSpec)) {
            sortSpecs.add(sortSpec);
          }
        }
      }
    }
    node.setSortSpecs(sortSpecs.toArray(new SortSpec[sortSpecs.size()]));

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
    if (newContext.targetListMgr.isEvaluated(referenceName)) {
      node.setQual(new FieldEval(target.getNamedColumn()));
    } else {
      node.setQual(target.getEvalTree());
      newContext.targetListMgr.markAsEvaluated(target);
    }

    return node;
  }

  public LogicalNode visitWindowAgg(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, WindowAggNode node,
                        Stack<LogicalNode> stack) throws PlanningException {
    Context newContext = new Context(context);

    if (node.hasPartitionKeys()) {
      for (Column c : node.getPartitionKeys()) {
        newContext.addNecessaryReferences(new FieldEval(c));
      }
    }

    if (node.hasSortSpecs()) {
      for (SortSpec sortSpec : node.getSortSpecs()) {
        newContext.addNecessaryReferences(new FieldEval(sortSpec.getSortKey()));
      }
    }

    for (WindowFunctionEval winFunc : node.getWindowFunctions()) {
      if (winFunc.hasSortSpecs()) {
        for (SortSpec sortSpec : winFunc.getSortSpecs()) {
          newContext.addNecessaryReferences(new FieldEval(sortSpec.getSortKey()));
        }
      }
    }


    int nonFunctionColumnNum = node.getTargets().length - node.getWindowFunctions().length;
    LinkedHashSet<String> nonFunctionColumns = Sets.newLinkedHashSet();
    for (int i = 0; i < nonFunctionColumnNum; i++) {
      FieldEval fieldEval = (new FieldEval(node.getTargets()[i].getNamedColumn()));
      nonFunctionColumns.add(newContext.addExpr(fieldEval));
    }

    final String [] aggEvalNames;
    if (node.hasAggFunctions()) {
      final int evalNum = node.getWindowFunctions().length;
      aggEvalNames = new String[evalNum];
      for (int evalIdx = 0, targetIdx = nonFunctionColumnNum; targetIdx < node.getTargets().length; evalIdx++,
          targetIdx++) {
        Target target = node.getTargets()[targetIdx];
        WindowFunctionEval winFunc = node.getWindowFunctions()[evalIdx];
        aggEvalNames[evalIdx] = newContext.addExpr(new Target(winFunc, target.getCanonicalName()));
      }
    } else {
      aggEvalNames = null;
    }

    // visit a child node
    LogicalNode child = super.visitWindowAgg(newContext, plan, block, node, stack);

    node.setInSchema(child.getOutSchema());

    List<Target> targets = Lists.newArrayList();
    if (nonFunctionColumnNum > 0) {
      for (String column : nonFunctionColumns) {
        Target target = context.targetListMgr.getTarget(column);

        // it rewrite grouping keys.
        // This rewrite sets right column names and eliminates duplicated grouping keys.
        if (context.targetListMgr.isEvaluated(column)) {
          targets.add(new Target(new FieldEval(target.getNamedColumn())));
        } else {
          if (target.getEvalTree().getType() == EvalType.FIELD) {
           targets.add(target);
          }
        }
      }
    }

    // Getting projected targets
    if (node.hasAggFunctions() && aggEvalNames != null) {
      WindowFunctionEval [] aggEvals = new WindowFunctionEval[aggEvalNames.length];
      int i = 0;
      for (Iterator<String> it = getFilteredReferences(aggEvalNames, TUtil.newList(aggEvalNames)); it.hasNext();) {

        String referenceName = it.next();
        Target target = context.targetListMgr.getTarget(referenceName);

        if (LogicalPlanner.checkIfBeEvaluatedAtWindowAgg(target.getEvalTree(), node)) {
          aggEvals[i++] = target.getEvalTree();
          context.targetListMgr.markAsEvaluated(target);

          targets.add(new Target(new FieldEval(target.getNamedColumn())));
        }
      }
      if (aggEvals.length > 0) {
        node.setWindowFunctions(aggEvals);
      }
    }

    node.setTargets(targets.toArray(new Target[targets.size()]));
    return node;
  }

  public LogicalNode visitGroupBy(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, GroupbyNode node,
                             Stack<LogicalNode> stack) throws PlanningException {
    Context newContext = new Context(context);

    // Getting grouping key names
    final int groupingKeyNum = node.getGroupingColumns().length;
    LinkedHashSet<String> groupingKeyNames = null;
    if (groupingKeyNum > 0) {
      groupingKeyNames = Sets.newLinkedHashSet();
      for (int i = 0; i < groupingKeyNum; i++) {
        FieldEval fieldEval = new FieldEval(node.getGroupingColumns()[i]);
        groupingKeyNames.add(newContext.addExpr(fieldEval));
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

    List<Target> targets = Lists.newArrayList();
    if (groupingKeyNum > 0 && groupingKeyNames != null) {
      // Restoring grouping key columns
      final List<Column> groupingColumns = new ArrayList<Column>();
      for (String groupingKey : groupingKeyNames) {
        Target target = context.targetListMgr.getTarget(groupingKey);

        // it rewrite grouping keys.
        // This rewrite sets right column names and eliminates duplicated grouping keys.
        if (context.targetListMgr.isEvaluated(groupingKey)) {
          Column c = target.getNamedColumn();
          if (!groupingColumns.contains(c)) {
            groupingColumns.add(c);
            targets.add(new Target(new FieldEval(target.getNamedColumn())));
          }
        } else {
          if (target.getEvalTree().getType() == EvalType.FIELD) {
            Column c = ((FieldEval)target.getEvalTree()).getColumnRef();
            if (!groupingColumns.contains(c)) {
              groupingColumns.add(c);
              targets.add(target);
              context.targetListMgr.markAsEvaluated(target);
            }
          } else {
            throw new PlanningException("Cannot evaluate this expression in grouping keys: " + target.getEvalTree());
          }
        }
      }

      node.setGroupingColumns(groupingColumns.toArray(new Column[groupingColumns.size()]));
    }

    // Getting projected targets
    if (node.hasAggFunctions() && aggEvalNames != null) {
      AggregationFunctionCallEval [] aggEvals = new AggregationFunctionCallEval[aggEvalNames.length];
      int i = 0;
      for (Iterator<String> it = getFilteredReferences(aggEvalNames, TUtil.newList(aggEvalNames)); it.hasNext();) {

        String referenceName = it.next();
        Target target = context.targetListMgr.getTarget(referenceName);

        if (LogicalPlanner.checkIfBeEvaluatedAtGroupBy(target.getEvalTree(), node)) {
          aggEvals[i++] = target.getEvalTree();
          context.targetListMgr.markAsEvaluated(target);
        }
      }
      if (aggEvals.length > 0) {
        node.setAggFunctions(aggEvals);
      }
    }
    Target [] finalTargets = buildGroupByTarget(node, targets, aggEvalNames);
    node.setTargets(finalTargets);

    LogicalPlanner.verifyProjectedFields(block, node);

    return node;
  }

  public static Target [] buildGroupByTarget(GroupbyNode groupbyNode, @Nullable List<Target> groupingKeyTargets,
                                             String [] aggEvalNames) {
    final int groupingKeyNum =
        groupingKeyTargets == null ? groupbyNode.getGroupingColumns().length : groupingKeyTargets.size();
    final int aggrFuncNum = aggEvalNames != null ? aggEvalNames.length : 0;
    EvalNode [] aggEvalNodes = groupbyNode.getAggFunctions();
    Target [] targets = new Target[groupingKeyNum + aggrFuncNum];

    if (groupingKeyTargets != null) {
      for (int groupingKeyIdx = 0; groupingKeyIdx < groupingKeyNum; groupingKeyIdx++) {
        targets[groupingKeyIdx] = groupingKeyTargets.get(groupingKeyIdx);
      }
    } else {
      for (int groupingKeyIdx = 0; groupingKeyIdx < groupingKeyNum; groupingKeyIdx++) {
        targets[groupingKeyIdx] = new Target(new FieldEval(groupbyNode.getGroupingColumns()[groupingKeyIdx]));
      }
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
    if (newContext.targetListMgr.isEvaluated(referenceName)) {
      node.setQual(new FieldEval(target.getNamedColumn()));
    } else {
      node.setQual(target.getEvalTree());
      newContext.targetListMgr.markAsEvaluated(target);
    }

    return node;
  }

  private static void pushDownIfComplexTermInJoinCondition(Context ctx, EvalNode cnf, EvalNode term)
      throws PlanningException {

    // If one of both terms in a binary operator is a complex expression, the binary operator will require
    // multiple phases. In this case, join cannot evaluate a binary operator.
    // So, we should prevent dividing the binary operator into more subexpressions.
    if (term.getType() != EvalType.FIELD &&
        !(term instanceof BinaryEval) &&
        !(term.getType() == EvalType.ROW_CONSTANT)) {
      String refName = ctx.addExpr(term);
      EvalTreeUtil.replace(cnf, term, new FieldEval(refName, term.getValueType()));
    }
  }

  public LogicalNode visitJoin(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, JoinNode node,
                          Stack<LogicalNode> stack) throws PlanningException {
    Context newContext = new Context(context);

    String joinQualReference = null;
    if (node.hasJoinQual()) {
      for (EvalNode eachQual : AlgebraicUtil.toConjunctiveNormalFormArray(node.getJoinQual())) {
        if (eachQual instanceof BinaryEval) {
          BinaryEval binaryQual = (BinaryEval) eachQual;

          for (int i = 0; i < 2; i++) {
            EvalNode term = binaryQual.getExpr(i);
            pushDownIfComplexTermInJoinCondition(newContext, eachQual, term);
          }
        }
      }

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
      if (newContext.targetListMgr.isEvaluated(joinQualReference)) {
        throw new PlanningException("Join condition must be evaluated in the proper Join Node: " + joinQualReference);
      } else {
        node.setJoinQual(target.getEvalTree());
        newContext.targetListMgr.markAsEvaluated(target);
      }
    }

    LinkedHashSet<Target> projectedTargets = Sets.newLinkedHashSet();
    for (Iterator<String> it = getFilteredReferences(context.targetListMgr.getNames(),
        context.requiredSet); it.hasNext();) {
      String referenceName = it.next();
      Target target = context.targetListMgr.getTarget(referenceName);

      if (context.targetListMgr.isEvaluated(referenceName)) {
        Target fieldReference = new Target(new FieldEval(target.getNamedColumn()));
        if (LogicalPlanner.checkIfBeEvaluatedAtJoin(block, fieldReference.getEvalTree(), node,
            stack.peek().getType() != NodeType.JOIN)) {
          projectedTargets.add(fieldReference);
        }
      } else if (LogicalPlanner.checkIfBeEvaluatedAtJoin(block, target.getEvalTree(), node,
          stack.peek().getType() != NodeType.JOIN)) {
        projectedTargets.add(target);
        context.targetListMgr.markAsEvaluated(target);
      }
    }

    node.setTargets(projectedTargets.toArray(new Target[projectedTargets.size()]));
    LogicalPlanner.verifyProjectedFields(block, node);
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

    LinkedHashSet<Target> projectedTargets = Sets.newLinkedHashSet();
    for (Iterator<Target> it = getFilteredTarget(targets, newContext.requiredSet); it.hasNext();) {
      Target target = it.next();
      newContext.addExpr(target);
    }

    for (Iterator<Target> it = context.targetListMgr.getFilteredTargets(newContext.requiredSet); it.hasNext();) {
      Target target = it.next();

      if (LogicalPlanner.checkIfBeEvaluatedAtRelation(block, target.getEvalTree(), node)) {
        projectedTargets.add(target);
        newContext.targetListMgr.markAsEvaluated(target);
      }
    }

    node.setTargets(projectedTargets.toArray(new Target[projectedTargets.size()]));
    LogicalPlanner.verifyProjectedFields(block, node);
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

    LinkedHashSet<Target> projectedTargets = Sets.newLinkedHashSet();
    for (Iterator<Target> it = getFilteredTarget(targets, newContext.requiredSet); it.hasNext();) {
      Target target = it.next();
      newContext.addExpr(target);
    }

    for (Iterator<Target> it = context.targetListMgr.getFilteredTargets(newContext.requiredSet); it.hasNext();) {
      Target target = it.next();

      if (LogicalPlanner.checkIfBeEvaluatedAtRelation(block, target.getEvalTree(), node)) {
        projectedTargets.add(target);
        newContext.targetListMgr.markAsEvaluated(target);
      }
    }

    node.setTargets(projectedTargets.toArray(new Target[projectedTargets.size()]));
    LogicalPlanner.verifyProjectedFields(block, node);
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

    Target [] targets;
    if (node.hasTargets()) {
      targets = node.getTargets();
    } else {
      targets = PlannerUtil.schemaToTargets(node.getOutSchema());
    }

    LinkedHashSet<Target> projectedTargets = Sets.newLinkedHashSet();
    for (Iterator<Target> it = getFilteredTarget(targets, upperContext.requiredSet); it.hasNext();) {
      Target target = it.next();
      upperContext.addExpr(target);
    }

    for (Iterator<Target> it = upperContext.targetListMgr.getFilteredTargets(upperContext.requiredSet); it.hasNext();) {
      Target target = it.next();

      if (LogicalPlanner.checkIfBeEvaluatedAtRelation(block, target.getEvalTree(), node)) {
        projectedTargets.add(target);
        upperContext.targetListMgr.markAsEvaluated(target);
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
