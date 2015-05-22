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

import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.AlgebraicUtil;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;
import org.apache.tajo.util.TUtil;

import java.util.*;

/**
 * Tajo's logical planner can generate different shapes of logical plans for the same query,
 * especially when the query involves one or more joins.
 * This class guarantees the plans' shape determinant.
 */
public class ExplainPlanPreprocessorForTest {
  private static final PlanShapeFixerContext shapeFixerContext = new PlanShapeFixerContext();
  private static final PlanShapeFixer shapeFixer = new PlanShapeFixer();
  private static final PidCollectorContext collectorContext = new PidCollectorContext();
  private static final JoinPidCollector joinPidCollector = new JoinPidCollector();
  private static final PidReseterContext resetContext = new PidReseterContext();
  private static final JoinPidReseter joinPidReseter = new JoinPidReseter();

  public void prepareTest(LogicalPlan plan) throws PlanningException {
    // Plan shape fixer
    shapeFixerContext.reset();
    shapeFixer.visit(shapeFixerContext, plan, plan.getRootBlock());

    /*
     * During join order optimization, new join nodes are created based on the chosen join order.
     * So, they have different pids for each query execution.
     * JoinPidCollector and JoinPidReseter reset the pids of join nodes.
     */
    collectorContext.reset();
    joinPidCollector.visit(collectorContext, plan, plan.getRootBlock());

    resetContext.reset(collectorContext.joinPids);
    joinPidReseter.visit(resetContext, plan, plan.getRootBlock());
  }

  private static class PlanShapeFixerContext {

    Stack<Integer> childNumbers = new Stack<Integer>();
    public void reset() {
      childNumbers.clear();
    }
  }

  /**
   * Given a commutative join, two children of the join node are interchangeable.
   * This class change the logical plan according to the following rules.
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
      node.setInSchema(sortSchema(node.getInSchema()));
      node.setOutSchema(sortSchema(node.getOutSchema()));
      context.childNumbers.push(context.childNumbers.pop() + 1);
      return null;
    }

    @Override
    public LogicalNode visitFilter(PlanShapeFixerContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                   SelectionNode node, Stack<LogicalNode> stack) throws PlanningException {
      super.visitFilter(context, plan, block, node, stack);
      if (node.hasQual()) {
        node.setQual(sortQual(node.getQual()));
      }
      return null;
    }

    @Override
    public LogicalNode visitScan(PlanShapeFixerContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 ScanNode node, Stack<LogicalNode> stack) throws PlanningException {
      super.visitScan(context, plan, block, node, stack);
      context.childNumbers.push(1);
      if (node.hasTargets()) {
        node.setTargets(sortTargets(node.getTargets()));
      }
      if (node.hasQual()) {
        node.setQual(sortQual(node.getQual()));
      }
      return null;
    }

    @Override
    public LogicalNode visitPartitionedTableScan(PlanShapeFixerContext context, LogicalPlan plan,
                                                 LogicalPlan.QueryBlock block, PartitionedTableScanNode node,
                                                 Stack<LogicalNode> stack)
        throws PlanningException {
      super.visitPartitionedTableScan(context, plan, block, node, stack);
      context.childNumbers.push(1);
      Path[] inputPaths = node.getInputPaths();
      Arrays.sort(inputPaths);
      node.setInputPaths(inputPaths);
      if (node.hasTargets()) {
        node.setTargets(sortTargets(node.getTargets()));
      }
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
          if (node.getLeftChild().getOutSchema().toString().compareTo(node.getRightChild().getOutSchema().toString()) <
              0) {
            swapChildren(node);
          }
        }
      }

      if (node.hasTargets()) {
        node.setTargets(sortTargets(node.getTargets()));
      }

      if (node.hasJoinQual()) {
        node.setJoinQual(sortQual(node.getJoinQual()));
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
      return sortQual(cnf);
    }

    private EvalNode sortQual(EvalNode[] cnf) {
      Arrays.sort(cnf, evalNodeComparator);
      return AlgebraicUtil.createSingletonExprFromCNF(cnf);
    }

    private Target[] sortTargets(Target[] targets) {
      Arrays.sort(targets, targetComparator);
      return targets;
    }

    private static void swapChildren(JoinNode node) {
      LogicalNode tmpChild = node.getLeftChild();
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

  private static class PidCollectorContext {
    List<Integer> joinPids = TUtil.newList();
    public void reset() {
      joinPids.clear();
    }
  }

  /**
   * {@link JoinPidCollector} collects the pids of all join
   * nodes.
   */
  private static class JoinPidCollector extends BasicLogicalPlanVisitor<PidCollectorContext, LogicalNode> {

    @Override
    public LogicalNode visitJoin(PidCollectorContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 JoinNode node, Stack<LogicalNode> stack) throws PlanningException {
      context.joinPids.add(node.getPID());
      super.visitJoin(context, plan, block, node, stack);

      return null;
    }
  }

  private static class PidReseterContext {
    List<Integer> joinPids;

    public void reset(List<Integer> joinPids) {
      this.joinPids = joinPids;
      Collections.sort(this.joinPids);
    }
  }

  /**
   * {@link JoinPidReseter} resets pids of join nodes with the pids collected by {@link JoinPidCollector} in ascending
   * order while traversing the query plan.
   */
  private static class JoinPidReseter extends BasicLogicalPlanVisitor<PidReseterContext, LogicalNode> {

    @Override
    public LogicalNode visitJoin(PidReseterContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 JoinNode node, Stack<LogicalNode> stack) throws PlanningException {
      super.visitJoin(context, plan, block, node, stack);
      node.setPID(context.joinPids.remove(0));
      
      return null;
    }
  }

}
