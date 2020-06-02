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
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.engine.planner.KeyProjector;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.expr.EvalTreeUtil;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.storage.FrameTuple;
import org.apache.tajo.storage.NullTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
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

  protected final KeyProjector leftKeyExtractor;
  protected final KeyProjector rightKeyExtractor;

  protected final List<Column[]> joinKeyPairs;

  protected final int rightNumCols;
  protected final int leftNumCols;

  protected final Column[] leftKeyList;
  protected final Column[] rightKeyList;

  protected final FrameTuple frameTuple;

  // projection
  protected Projector projector;

  public CommonJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec outer, PhysicalExec inner) {
    super(context, SchemaUtil.merge(outer.getSchema(), inner.getSchema()),
        plan.getOutSchema(), outer, inner);
    this.plan = plan;
    this.leftSchema = outer.getSchema();
    this.rightSchema = inner.getSchema();
    if (plan.hasJoinQual()) {
      EvalNode[] extracted = EvalTreeUtil.extractJoinConditions(plan.getJoinQual(), leftSchema, rightSchema);
      joinQual = extracted[0];
      leftJoinFilter = extracted[1];
      rightJoinFilter = extracted[2];
    }
    this.hasJoinQual = joinQual != null;

    // for projection
    this.projector = new Projector(context, inSchema, outSchema, plan.getTargets());

    // for join
    this.frameTuple = new FrameTuple();

    switch (plan.getJoinType()) {

      case CROSS:
        if (hasJoinQual) {
          throw new TajoInternalError("Cross join cannot evaluate join conditions.");
        } else {
          joinKeyPairs = null;
          rightNumCols = leftNumCols = -1;
          leftKeyList = rightKeyList = null;
          leftKeyExtractor = null;
          rightKeyExtractor = null;
        }
        break;

      case INNER:
        // Other join types except INNER join can have empty join condition.
        if (!hasJoinQual) {
          throw new TajoInternalError("Inner join must have any join conditions.");
        }
      default:
        // HashJoin only can manage equi join key pairs.
        this.joinKeyPairs = PlannerUtil.getJoinKeyPairs(joinQual, outer.getSchema(),
            inner.getSchema(), false);

        leftKeyList = new Column[joinKeyPairs.size()];
        rightKeyList = new Column[joinKeyPairs.size()];

        for (int i = 0; i < joinKeyPairs.size(); i++) {
          leftKeyList[i] = outer.getSchema().getColumn(joinKeyPairs.get(i)[0].getQualifiedName());
          rightKeyList[i] = inner.getSchema().getColumn(joinKeyPairs.get(i)[1].getQualifiedName());
        }

        leftNumCols = outer.getSchema().size();
        rightNumCols = inner.getSchema().size();

        leftKeyExtractor = new KeyProjector(leftSchema, leftKeyList);
        rightKeyExtractor = new KeyProjector(rightSchema, rightKeyList);
        break;
    }
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
    return leftJoinFilter != null && !leftJoinFilter.eval(left).isTrue();
  }

  /**
   * Evaluate an input tuple with a right join filter
   *
   * @param right Tuple to be evaluated
   * @return True if an input tuple is matched to the right join filter
   */
  protected boolean rightFiltered(Tuple right) {
    return rightJoinFilter != null && !rightJoinFilter.eval(right).isTrue();
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
    return Iterators.filter(rightTuples.iterator(), input -> {
      return rightJoinFilter.eval(input).isTrue();
    });
  }

  /**
   * Create a list that contains a single null tuple.
   *
   * @param width the width of null tuple which will be created.
   * @return created list of a null tuple
   */
  protected List<Tuple> nullTupleList(int width) {
    return Collections.singletonList(NullTuple.create(width));
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
