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

package org.apache.tajo.plan.rewrite.rules;

import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlan.QueryBlock;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRule;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRuleContext;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.stream.Collectors;

/**
 *
 * This rule must be applied after in-subquery rewrite rule and join order optimization.
 */
public class EarlyNullFilterForJoinRule implements LogicalPlanRewriteRule {
  private final static String NAME = EarlyNullFilterForJoinRule.class.getSimpleName();

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean isEligible(LogicalPlanRewriteRuleContext context) {
    return context.getPlan().getQueryBlocks().stream().anyMatch(b -> b.hasNode(NodeType.JOIN));
  }

  @Override
  public LogicalPlan rewrite(LogicalPlanRewriteRuleContext context) throws TajoException {
    Rewriter rewriter = new Rewriter();
    rewriter.visit(new Context(), context.getPlan(), context.getPlan().getRootBlock());
    return context.getPlan();
  }

  private static class Context {
    final Map<Column, EvalNode> generatedFilters = new HashMap<>();
  }

  private static class Rewriter extends BasicLogicalPlanVisitor<Context, LogicalNode> {

    @Override
    public LogicalNode visitJoin(Context context, LogicalPlan plan, QueryBlock block, JoinNode join,
                                 Stack<LogicalNode> stack) throws TajoException {
      EvalNode realQual = EvalTreeUtil.extractJoinConditions(join.getJoinQual(), join.getLeftChild().getOutSchema(), join.getRightChild().getOutSchema())[0];
      context.generatedFilters.putAll(new NullFilterGenerator(realQual).gen()); // duplicated filters for the same column will be removed.

      super.visitJoin(context, plan, block, join, stack);

      return join;
    }

    @Override
    public LogicalNode visitScan(Context context, LogicalPlan plan, QueryBlock block, ScanNode scan,
                                 Stack<LogicalNode> stack) throws TajoException {
      super.visitScan(context, plan, block, scan, stack);
      Schema schema = scan.getPhysicalSchema(); // include partition columns
      List<EvalNode> founds = context.generatedFilters.entrySet().stream()
          .filter(e -> schema.contains(e.getKey()))
          .map(e -> e.getValue())
          .collect(Collectors.toList());

      if (founds.size() > 0) {
        EvalNode nullFilter = AlgebraicUtil.createSingletonExprFromCNF(founds);
        if (scan.hasQual()) {
          scan.setQual(AlgebraicUtil.createSingletonExprFromCNF(nullFilter, scan.getQual()));
        } else {
          scan.setQual(nullFilter);
        }
      }
      return scan;
    }
  }

  private static class FilterGenContext {
    private final Map<Column, EvalNode> generatedFilters = new HashMap<>();
  }

  private static class NullFilterGenerator extends SimpleEvalNodeVisitor<FilterGenContext> {
    final EvalNode joinQual;

    public NullFilterGenerator(EvalNode joinQual) {
      this.joinQual = joinQual;
    }

    public Map<Column, EvalNode> gen() {
      FilterGenContext context = new FilterGenContext();
      this.visit(context, joinQual, new Stack<>());
      return context.generatedFilters;
    }

    @Override
    protected EvalNode visitField(FilterGenContext context, FieldEval evalNode, Stack<EvalNode> stack) {
      context.generatedFilters.put(evalNode.getColumnRef(), genNullFilter(evalNode));
      return evalNode;
    }

    private static EvalNode genNullFilter(FieldEval eval) {
      return new IsNullEval(true, eval);
    }
  }
}
