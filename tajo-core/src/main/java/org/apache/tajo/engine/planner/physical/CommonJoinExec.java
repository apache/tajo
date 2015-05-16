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

package org.apache.tajo.engine.planner.physical;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.plan.expr.AlgebraicUtil;
import org.apache.tajo.plan.expr.BinaryEval;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.expr.EvalTreeUtil;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.storage.FrameTuple;
import org.apache.tajo.storage.NullTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * common exec for all join execs
 */
public abstract class CommonJoinExec extends BinaryPhysicalExec {

  // from logical plan
  protected JoinNode plan;
  protected final boolean hasJoinQual;

  protected EvalNode joinQual;         // ex) a.id = b.id
  protected EvalNode leftJoinFilter;   // ex) a > 10
  protected EvalNode rightJoinFilter;  // ex) b > 5

  protected final Schema leftSchema;
  protected final Schema rightSchema;

  protected final FrameTuple frameTuple;
  protected final Tuple outTuple;

  // projection
  protected Projector projector;

  public CommonJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec outer, PhysicalExec inner) {
    super(context, SchemaUtil.merge(outer.getSchema(), inner.getSchema()),
        plan.getOutSchema(), outer, inner);
    this.plan = plan;
    this.leftSchema = outer.getSchema();
    this.rightSchema = inner.getSchema();
    if (plan.hasJoinQual()) {
      EvalNode[] extracted = extractJoinConditions(plan.getJoinQual(), leftSchema, rightSchema);
      joinQual = extracted[0];
      leftJoinFilter = extracted[1];
      rightJoinFilter = extracted[2];
    }
    this.hasJoinQual = joinQual != null;

    // for projection
    this.projector = new Projector(context, inSchema, outSchema, plan.getTargets());

    // for join
    this.frameTuple = new FrameTuple();
    this.outTuple = new VTuple(outSchema.size());
  }

  /**
   * It separates a singular CNF-formed join condition into a join condition, a left join filter, and
   * right join filter.
   *
   * @param joinQual the original join condition
   * @param leftSchema Left table schema
   * @param rightSchema Left table schema
   * @return Three element EvalNodes, 0 - join condition, 1 - left join filter, 2 - right join filter.
   */
  private EvalNode[] extractJoinConditions(EvalNode joinQual, Schema leftSchema, Schema rightSchema) {
    List<EvalNode> joinQuals = Lists.newArrayList();
    List<EvalNode> leftFilters = Lists.newArrayList();
    List<EvalNode> rightFilters = Lists.newArrayList();
    for (EvalNode eachQual : AlgebraicUtil.toConjunctiveNormalFormArray(joinQual)) {
      if (!(eachQual instanceof BinaryEval)) {
        continue; // todo 'between', etc.
      }
      BinaryEval binaryEval = (BinaryEval)eachQual;
      LinkedHashSet<Column> leftColumns = EvalTreeUtil.findUniqueColumns(binaryEval.getLeftExpr());
      LinkedHashSet<Column> rightColumns = EvalTreeUtil.findUniqueColumns(binaryEval.getRightExpr());
      boolean leftInLeft = leftSchema.containsAny(leftColumns);
      boolean rightInLeft = leftSchema.containsAny(rightColumns);
      boolean leftInRight = rightSchema.containsAny(leftColumns);
      boolean rightInRight = rightSchema.containsAny(rightColumns);

      boolean columnsFromLeft = leftInLeft || rightInLeft;
      boolean columnsFromRight = leftInRight || rightInRight;
      if (!columnsFromLeft && !columnsFromRight) {
        continue; // todo constant expression : this should be done in logical phase
      }
      if (columnsFromLeft ^ columnsFromRight) {
        if (columnsFromLeft) {
          leftFilters.add(eachQual);
        } else {
          rightFilters.add(eachQual);
        }
        continue;
      }
      if ((leftInLeft && rightInLeft) || (leftInRight && rightInRight)) {
        continue; // todo not allowed yet : this should be checked in logical phase
      }
      joinQuals.add(eachQual);
    }
    return new EvalNode[] {
        joinQuals.isEmpty() ? null : AlgebraicUtil.createSingletonExprFromCNF(joinQuals),
        leftFilters.isEmpty() ? null : AlgebraicUtil.createSingletonExprFromCNF(leftFilters),
        rightFilters.isEmpty() ? null : AlgebraicUtil.createSingletonExprFromCNF(rightFilters)
    };
  }

  public JoinNode getPlan() {
    return plan;
  }

  /**
   * Evaluate an input tuple with a left join filter
   *
   * @param left Tuple to be evaluated
   * @return True if an input tuple is matched to the left join filter
   */
  protected boolean leftFiltered(Tuple left) {
    return leftJoinFilter != null && !leftJoinFilter.eval(left).asBool();
  }

  /**
   * Evaluate an input tuple with a right join filter
   *
   * @param right Tuple to be evaluated
   * @return True if an input tuple is matched to the right join filter
   */
  protected boolean rightFiltered(Tuple right) {
    return rightJoinFilter != null && !rightJoinFilter.eval(right).asBool();
  }

  /**
   * Return an tuple iterator filters rows in a right table by using a join filter.
   * It must takes rows of a right table.
   *
   * @param rightTuples Tuple iterator
   * @return rows Filtered by a join filter on right table.
   */
  protected Iterator<Tuple> rightFiltered(Iterable<Tuple> rightTuples) {
    if (rightTuples == null) {
      return Iterators.emptyIterator();
    }
    if (rightJoinFilter == null) {
      return rightTuples.iterator();
    }
    return Iterators.filter(rightTuples.iterator(), new Predicate<Tuple>() {
      @Override
      public boolean apply(Tuple input) {
        return rightJoinFilter.eval(input).asBool();
      }
    });
  }

  /**
   * Return an tuple iterator, containing a single NullTuple
   *
   * @param width the width of tuple
   * @return an tuple iterator, containing a single NullTuple
   */
  protected Iterator<Tuple> nullIterator(int width) {
    return Arrays.asList(NullTuple.create(width)).iterator();
  }

  @Override
  public void init() throws IOException {
    super.init();
    if (hasJoinQual) {
      joinQual.bind(context.getEvalContext(), inSchema);
    }
    if (leftJoinFilter != null) {
      leftJoinFilter.bind(context.getEvalContext(), leftSchema);
    }
    if (rightJoinFilter != null) {
      rightJoinFilter.bind(context.getEvalContext(), rightSchema);
    }
  }

  @Override
  protected void compile() {
    if (hasJoinQual) {
      joinQual = context.getPrecompiledEval(inSchema, joinQual);
    }
    // compile filters?
  }

  @Override
  public void close() throws IOException {
    super.close();
    plan = null;
    joinQual = null;
    leftJoinFilter = null;
    rightJoinFilter = null;
    projector = null;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " [" + leftSchema + " : " + rightSchema + "]";
  }
}
