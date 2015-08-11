package org.apache.tajo.plan.rewrite.rules;

import org.apache.tajo.exception.TajoException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.plan.logical.SelectionNode;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRule;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRuleContext;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;
import org.apache.tajo.util.TUtil;

import java.util.List;
import java.util.Set;
import java.util.Stack;

/**
 * Condition reduce rule reduces the condition based on distributivity.
 */
public class ConditionReduceRule implements LogicalPlanRewriteRule {

  @Override
  public String getName() {
    return "ConditionReduceRule";
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
  public LogicalPlan rewrite(LogicalPlanRewriteRuleContext context) throws TajoException {

    return null;
  }

  private final static class Rewriter extends BasicLogicalPlanVisitor<Object, LogicalNode> {

    @Override
    public LogicalNode visitFilter(Object context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                   SelectionNode selNode, Stack<LogicalNode> stack) throws TajoException {

      return null;
    }

    @Override
    public LogicalNode visitJoin(Object context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 JoinNode joinNode, Stack<LogicalNode> stack) throws TajoException {
      return null;
    }

//    private EvalNode findCommonCnfs(EvalNode[] dnfs) {
//
//      // Find common CNFs among DNFs.
//      List<Set<EvalNode>> cnfsOfEachDnf = TUtil.newList();
//      for (EvalNode eachDnf : dnfs) {
//        cnfsOfEachDnf.add(TUtil.newHashSet(AlgebraicUtil.toConjunctiveNormalFormArray(eachDnf)));
//      }
//
//      Set<EvalNode> cnfsOfFirstDnf = cnfsOfEachDnf.get(0);
//      Set<EvalNode> commonCnfs = TUtil.newHashSet();
//      int i;
//
//      for (EvalNode eachCnfOfFirstDnf : cnfsOfFirstDnf) {
//        for (i = 1; i < dnfs.length; i++) {
//          if (!cnfsOfEachDnf.get(i).contains(eachCnfOfFirstDnf)) {
//            break;
//          }
//        }
//        if (i == dnfs.length) {
//          commonCnfs.add(eachCnfOfFirstDnf);
//        }
//      }
//
//
//
//      return null;
//    }
//
//    private EvalNode findCommonDnfs(EvalNode[] cnfs) {
//      return null;
//    }
  }

  private static class EvalRewriter extends BasicEvalNodeVisitor<Object, EvalNode> {

    @Override
    public EvalNode visitAnd(Object context, BinaryEval evalNode, Stack<EvalNode> stack) {
      EvalNode child = super.visitAnd(context, evalNode, stack);

      if (evalNode.getLeftExpr().getType() == EvalType.OR &&
          evalNode.getRightExpr().getType() == EvalType.OR) {

        // Find common quals from the left and right children.
        Set<EvalNode> commonQuals = TUtil.newHashSet();
        Set<EvalNode> childsOfLeft = TUtil.newHashSet(evalNode.getLeftExpr().getChild(0),
            evalNode.getLeftExpr().getChild(1));
        for (int i = 0; i < 2; i++) {
          if (childsOfLeft.contains(evalNode.getRightExpr().getChild(i))) {
            commonQuals.add(evalNode.getRightExpr().getChild(i));
          }
        }

        if (commonQuals.size() == 2) {
          // Current binary eval has the same left and right children, so it is useless.
          // Connect the parent of the current eval and one of the children directly.

        } else if (commonQuals.size() == 1) {
          // A single common qual is found.
          // ( a | b ) ^ ( a | c ) -> a | (b ^ c)

          if (evalNode.getLeftExpr())
        }
      }

      return null;
    }

    @Override
    public EvalNode visitOr(Object context, BinaryEval evalNode, Stack<EvalNode> stack) {
      EvalNode child = super.visitOr(context, evalNode, stack);

      return null;
    }

  }
}
