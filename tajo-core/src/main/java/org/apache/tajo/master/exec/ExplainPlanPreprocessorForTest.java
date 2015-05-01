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

package org.apache.tajo.master.exec;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.AlgebraicUtil;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Stack;

/**
 * Tajo's logical planner can generate different shapes of logical plans for the same query,
 * especially when the query involves one or more joins.
 * This class guarantees the consistency of the logical plan for the same query.
 */
public class ExplainPlanPreprocessorForTest {
  private static final PlanShapeFixerContext shapeFixerContext = new PlanShapeFixerContext();
  private static final PlanShapeFixer shapeFixer = new PlanShapeFixer();
  private static final PidResetContext resetContext = new PidResetContext();
  private static final PidReseter pidReseter = new PidReseter();

  public void prepareTest(LogicalPlan plan) throws PlanningException {
    // Pid reseter
    resetContext.reset();
    pidReseter.visit(resetContext, plan, plan.getRootBlock());

    // Plan shape fixer
    shapeFixerContext.reset();
    shapeFixer.visit(shapeFixerContext, plan, plan.getRootBlock());
  }

  private static class PlanShapeFixerContext {

    Stack<Integer> childNumbers = new Stack<Integer>();
    public void reset() {
      childNumbers.clear();
    }
  }

  /**
   * Given a commutative join, two children of the join node are interchangeable.
   * This class fix the logical plan according to the following rules.
   *
   * <h3>Rules</h3>
   * <ul>
   *   <li>When one of the both children has more descendants,
   *   change the plan in order that the left child is the one who has more descendants.</li>
   *   <li>When both children have the same number of descendants,
   *   their order is decided based on their string representation.</li>
   * </ul>
   *
   * In addition, in/out schemas, quals, and targets are sorted by their names.
   */
  private static class PlanShapeFixer extends BasicLogicalPlanVisitor<PlanShapeFixerContext, LogicalNode> {
    private static final ColumnComparator columnComparator = new ColumnComparator();
    private static final EvalNodeComparator evalNodeComparator = new EvalNodeComparator();
    private static final TargetComparator targetComparator = new TargetComparator();

    @Override
    public LogicalNode visit(PlanShapeFixerContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                             LogicalNode node, Stack<LogicalNode> stack) throws PlanningException {
      super.visit(context, plan, block, node, stack);
      context.childNumbers.push(context.childNumbers.pop()+1);
      return null;
    }

    @Override
    public LogicalNode visitScan(PlanShapeFixerContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 ScanNode node, Stack<LogicalNode> stack) throws PlanningException {
      super.visitScan(context, plan, block, node, stack);
      context.childNumbers.push(1);
      node.setInSchema(sortSchema(node.getInSchema()));
      if (node.hasQual()) {
        node.setQual(sortQual(node.getQual()));
      }
      return null;
    }

    @Override
    public LogicalNode visitJoin(PlanShapeFixerContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 JoinNode node, Stack<LogicalNode> stack) throws PlanningException {
      super.visitJoin(context, plan, block, node, stack);
      int rightChildNum = context.childNumbers.pop();
      int leftChildNum = context.childNumbers.pop();

      if (PlannerUtil.isCommutativeJoin(node.getJoinType())) {

        if (leftChildNum < rightChildNum) {
          swapChildren(node);
        } else if (leftChildNum == rightChildNum) {
          if (node.getLeftChild().toString().compareTo(node.getRightChild().toString()) <
              0) {
            swapChildren(node);
          }
        }
      }

      node.setInSchema(sortSchema(node.getInSchema()));
      node.setOutSchema(sortSchema(node.getOutSchema()));

      if (node.hasJoinQual()) {
        node.setJoinQual(sortQual(node.getJoinQual()));
      }

      if (node.hasTargets()) {
        node.setTargets(sortTargets(node.getTargets()));
      }

      context.childNumbers.push(rightChildNum + leftChildNum);

      return null;
    }

    private Schema sortSchema(Schema schema) {
      Column[] columns = schema.toArray();
      Arrays.sort(columns, columnComparator);

      Schema sorted = new Schema();
      for (Column col : columns) {
        sorted.addColumn(col);
      }
      return sorted;
    }

    private EvalNode sortQual(EvalNode qual) {
      EvalNode[] cnf = AlgebraicUtil.toConjunctiveNormalFormArray(qual);
      Arrays.sort(cnf, evalNodeComparator);
      return AlgebraicUtil.createSingletonExprFromCNF(cnf);
    }

    private Target[] sortTargets(Target[] targets) {
      Arrays.sort(targets, targetComparator);
      return targets;
    }

    private static void swapChildren(JoinNode node) {
      LogicalNode tmpChild = node.getLeftChild();
      int tmpId = tmpChild.getPID();
      tmpChild.setPID(node.getRightChild().getPID());
      node.getRightChild().setPID(tmpId);
      node.setLeftChild(node.getRightChild());
      node.setRightChild(tmpChild);
    }
  }

  public static class ColumnComparator implements Comparator<Column> {

    @Override
    public int compare(Column o1, Column o2) {
      return o1.getQualifiedName().compareTo(o2.getQualifiedName());
    }
  }

  private static class EvalNodeComparator implements Comparator<EvalNode> {

    @Override
    public int compare(EvalNode o1, EvalNode o2) {
      return o1.toJson().compareTo(o2.toJson());
    }
  }

  private static class TargetComparator implements Comparator<Target> {

    @Override
    public int compare(Target o1, Target o2) {
      return o1.toJson().compareTo(o2.toJson());
    }
  }

  private static class PidResetContext {
    int seqId = 0;
    public void reset() {
      seqId = 0;
    }
  }

  /**
   * During join order optimization, new join nodes are created based on the chosen join order.
   * So, each join node has different pids.
   * This class sequentially assigns unique pids to all logical nodes.
   */
  private static class PidReseter extends BasicLogicalPlanVisitor<PidResetContext, LogicalNode> {

    @Override
    public void preHook(LogicalPlan plan, LogicalNode node, Stack<LogicalNode> stack, PidResetContext context)
        throws PlanningException {
      node.setPID(context.seqId++);
    }
  }

}
