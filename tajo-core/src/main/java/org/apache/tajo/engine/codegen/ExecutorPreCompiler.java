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

package org.apache.tajo.engine.codegen;

import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.storage.BaseTupleComparator;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.util.Pair;

import java.util.Collections;
import java.util.Map;
import java.util.Stack;

public class ExecutorPreCompiler extends BasicLogicalPlanVisitor<ExecutorPreCompiler.CompilationContext, LogicalNode> {
  private static final Log LOG = LogFactory.getLog(ExecutorPreCompiler.class);

  private static final ExecutorPreCompiler instance;

  static {
    instance = new ExecutorPreCompiler();
  }

  public static void compile(CompilationContext context, LogicalNode node) throws PlanningException {
    instance.visit(context, null, null, node, new Stack<LogicalNode>());
    context.compiledEvals = Collections.unmodifiableMap(context.compiledEvals);
  }

  public static Map<Pair<Schema, EvalNode>, EvalNode> compile(TajoClassLoader classLoader, LogicalNode node)
      throws PlanningException {
    CompilationContext context = new CompilationContext(classLoader);
    instance.visit(context, null, null, node, new Stack<LogicalNode>());
    return context.compiledEvals;
  }

  public static class CompilationContext {
    private final EvalNodeCompiler evalCompiler;
    private final TupleComparerCompiler comparerCompiler;
    private Map<Pair<Schema,EvalNode>, EvalNode> compiledEvals;
    private Map<Pair<Schema,BaseTupleComparator>, TupleComparator> unsafeComparators;
    private Map<Pair<Schema,BaseTupleComparator>, TupleComparator> comparators;

    public CompilationContext(TajoClassLoader classLoader) {
      this.evalCompiler = new EvalNodeCompiler(classLoader);
      this.comparerCompiler = new TupleComparerCompiler(classLoader);
      this.compiledEvals = Maps.newHashMap();
      this.unsafeComparators = Maps.newHashMap();
      this.comparators = Maps.newHashMap();
    }

    public EvalNodeCompiler getEvalCompiler() {
      return evalCompiler;
    }

    public TupleComparerCompiler getComparatorCompiler() {
      return comparerCompiler;
    }

    public Map<Pair<Schema, EvalNode>, EvalNode> getPrecompiedEvals() {
      return compiledEvals;
    }

    public Map<Pair<Schema, BaseTupleComparator>, TupleComparator> getUnSafeComparators() {
      return unsafeComparators;
    }

    public Map<Pair<Schema, BaseTupleComparator>, TupleComparator> getComparators() {
      return comparators;
    }
  }

  private static void compileIfAbsent(CompilationContext context, Schema schema, EvalNode eval) {
    Pair<Schema, EvalNode> key = new Pair<Schema, EvalNode>(schema, eval);
    if (!context.compiledEvals.containsKey(key)) {
      try {
        EvalNode compiled = context.evalCompiler.compile(schema, eval);
        context.compiledEvals.put(key, compiled);

      } catch (Throwable t) {
        // If any compilation error occurs, it works in a fallback mode. This mode just uses EvalNode objects
        // instead of a compiled EvalNode.
        context.compiledEvals.put(key, eval);
        LOG.warn(t);
      }
    }
  }

  private static void compileIfAbsent(CompilationContext context, Schema schema, BaseTupleComparator comparator) {
    Pair<Schema, BaseTupleComparator> key = new Pair<Schema, BaseTupleComparator>(schema, comparator);

    if (!context.unsafeComparators.containsKey(key)) {
      TupleComparator unsafeComparator = context.comparerCompiler.compile(comparator, true);
      context.unsafeComparators.put(key, unsafeComparator);
    }

    if (!context.comparators.containsKey(key)) {
      TupleComparator compiledComparator = context.comparerCompiler.compile(comparator, false);
      context.comparators.put(key, compiledComparator);
    }
  }

  private static void compileProjectableNode(CompilationContext context, Schema schema, Projectable node) {
    Target[] targets;
    if (node.hasTargets()) {
      targets = node.getTargets();
    } else {
      targets = PlannerUtil.schemaToTargets(node.getOutSchema());
    }

    for (Target target : targets) {
      compileIfAbsent(context, schema, target.getEvalTree());
    }
  }

  private static void compileSelectableNode(CompilationContext context, Schema schema, SelectableNode node) {
    if (node.hasQual()) {
      compileIfAbsent(context, schema, node.getQual());
    }
  }

  @Override
  public LogicalNode visitProjection(CompilationContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                     ProjectionNode node, Stack<LogicalNode> stack) throws PlanningException {
    super.visitProjection(context, plan, block, node, stack);

    compileProjectableNode(context, node.getInSchema(), node);

    return node;
  }

  @Override
  public LogicalNode visitSort(CompilationContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                               SortNode node, Stack<LogicalNode> stack) throws PlanningException {
    super.visitSort(context, plan, block, node, stack);

    BaseTupleComparator comparator = new BaseTupleComparator(node.getInSchema(), node.getSortKeys());
    compileIfAbsent(context, node.getInSchema(), comparator);

    return node;
  }

  @Override
  public LogicalNode visitHaving(CompilationContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 HavingNode node, Stack<LogicalNode> stack) throws PlanningException {
    super.visitHaving(context, plan, block, node, stack);

    compileSelectableNode(context, node.getInSchema(), node);

    return node;
  }

  @Override
  public LogicalNode visitGroupBy(CompilationContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                  GroupbyNode node, Stack<LogicalNode> stack) throws PlanningException {
    super.visitGroupBy(context, plan, block, node, stack);
    // Groupby executors do not use Projector.
    return node;
  }

  @Override
  public LogicalNode visitWindowAgg(CompilationContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                    WindowAggNode node, Stack<LogicalNode> stack) throws PlanningException {
    super.visitWindowAgg(context, plan, block, node, stack);

    compileProjectableNode(context, node.getInSchema(), node);

    return node;
  }

  public LogicalNode visitDistinct(CompilationContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                   DistinctGroupbyNode node, Stack<LogicalNode> stack) throws PlanningException {
    super.visitDistinct(context, plan, block, node, stack);

    compileProjectableNode(context, node.getInSchema(), node);
    return node;
  }

  @Override
  public LogicalNode visitFilter(CompilationContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 SelectionNode node, Stack<LogicalNode> stack) throws PlanningException {
    super.visitFilter(context, plan, block, node, stack);

    compileSelectableNode(context, node.getInSchema(), node);

    return node;
  }

  @Override
  public LogicalNode visitJoin(CompilationContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                               JoinNode node, Stack<LogicalNode> stack) throws PlanningException {
    super.visitJoin(context, plan, block, node, stack);

    compileProjectableNode(context, node.getInSchema(), node);

    if (node.hasJoinQual()) {
      compileIfAbsent(context, node.getInSchema(), node.getJoinQual());
    }

    return node;
  }

  @Override
  public LogicalNode visitTableSubQuery(CompilationContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                   TableSubQueryNode node, Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    visit(context, plan, null, node.getSubQuery(), stack);
    stack.pop();

    if (node.hasTargets()) {
      for (Target target : node.getTargets()) {
        compileIfAbsent(context, node.getTableSchema(), target.getEvalTree());
      }
    }

    return node;
  }

  @Override
  public LogicalNode visitPartitionedTableScan(CompilationContext context, LogicalPlan plan,
                                               LogicalPlan.QueryBlock block, PartitionedTableScanNode node,
                                               Stack<LogicalNode> stack) throws PlanningException {
    visitScan(context, plan, block, node, stack);
    return node;
  }

  @Override
  public LogicalNode visitScan(CompilationContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                               ScanNode node, Stack<LogicalNode> stack) throws PlanningException {

    compileProjectableNode(context, node.getInSchema(), node);
    compileSelectableNode(context, node.getInSchema(), node);

    return node;
  }
}
