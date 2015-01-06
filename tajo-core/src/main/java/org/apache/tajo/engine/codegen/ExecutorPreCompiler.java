/*
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
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;
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
    context.compiledEval = Collections.unmodifiableMap(context.compiledEval);
  }

  public static Map<Pair<Schema, EvalNode>, EvalNode> compile(TajoClassLoader classLoader, LogicalNode node)
      throws PlanningException {
    CompilationContext context = new CompilationContext(classLoader);
    instance.visit(context, null, null, node, new Stack<LogicalNode>());
    return context.compiledEval;
  }

  public static class CompilationContext {
    private final EvalCodeGenerator compiler;
    private Map<Pair<Schema,EvalNode>, EvalNode> compiledEval;

    public CompilationContext(TajoClassLoader classLoader) {
      this.compiler = new EvalCodeGenerator(classLoader);
      this.compiledEval = Maps.newHashMap();
    }

    public EvalCodeGenerator getCompiler() {
      return compiler;
    }

    public Map<Pair<Schema, EvalNode>, EvalNode> getPrecompiedEvals() {
      return compiledEval;
    }
  }

  private static void compileIfAbsent(CompilationContext context, Schema schema, EvalNode eval) {
    Pair<Schema, EvalNode> key = new Pair<Schema, EvalNode>(schema, eval);
    if (!context.compiledEval.containsKey(key)) {
      try {
        EvalNode compiled = context.compiler.compile(schema, eval);
        context.compiledEval.put(key, compiled);

      } catch (Throwable t) {
        // If any compilation error occurs, it works in a fallback mode. This mode just uses EvalNode objects
        // instead of a compiled EvalNode.
        context.compiledEval.put(key, eval);
        LOG.warn(t);
      }
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

    compileProjectableNode(context, node.getInSchema(), node);

    return node;
  }

  @Override
  public LogicalNode visitWindowAgg(CompilationContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                    WindowAggNode node, Stack<LogicalNode> stack) throws PlanningException {
    super.visitWindowAgg(context, plan, block, node, stack);

    compileProjectableNode(context, node.getInSchema(), node);

    return node;
  }

  public LogicalNode visitDistinctGroupby(CompilationContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                          DistinctGroupbyNode node, Stack<LogicalNode> stack) throws PlanningException {
    super.visitDistinctGroupby(context, plan, block, node, stack);

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
        compileIfAbsent(context, node.getLogicalSchema(), target.getEvalTree());
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
