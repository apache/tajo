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
import com.google.common.base.Predicates;
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
import org.apache.tajo.plan.expr.EvalType;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.storage.FrameTuple;
import org.apache.tajo.storage.NullTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

// common exec for all join execs
public abstract class CommonJoinExec extends BinaryPhysicalExec {

  // from logical plan
  protected JoinNode plan;

  protected EvalNode equiQual;     // ex) a.id = b.id
  protected EvalNode joinQual;     // ex) a.id = b.id
  protected EvalNode thetaQual;    // ex) a.id > b.id
  protected EvalNode leftFilter;   // ex) a > 10
  protected EvalNode rightFilter;  // ex) b > 5

  protected final Predicate<Tuple> leftPredicate;
  protected final Predicate<Tuple> rightPredicate;

  protected final Schema leftSchema;
  protected final Schema rightSchema;

  protected final ThetaOperation thetaOp;
  protected final FrameTuple frameTuple;
  protected final Tuple outTuple;

  // projection
  protected final Projector projector;

  public CommonJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec outer, PhysicalExec inner) {
    super(context, SchemaUtil.merge(outer.getSchema(), inner.getSchema()),
        plan.getOutSchema(), outer, inner);
    this.plan = plan;
    this.leftSchema = outer.getSchema();
    this.rightSchema = inner.getSchema();
    if (plan.hasJoinQual()) {
      EvalNode[] extracted = extractJoinConditions(plan.getJoinQual(), leftSchema, rightSchema);
      equiQual = extracted[0];
      joinQual = extracted[1];
      leftFilter = extracted[2];
      rightFilter = extracted[3];
      thetaQual = extracted[4];
    }

    this.thetaOp = thetaQual == null ? null : ThetaOperation.valueOf(thetaQual);
    this.leftPredicate = toPredicate(leftFilter);
    this.rightPredicate = toPredicate(rightFilter);

    // for projection
    this.projector = new Projector(context, inSchema, outSchema, plan.getTargets());

    // for join
    this.frameTuple = new FrameTuple();
    this.outTuple = new VTuple(outSchema.size());
  }

  // non-equi theta join
  public boolean isThetaJoin() {
    return thetaOp != null;
  }

  public boolean isHashJoin() {
    return false;
  }

  private EvalNode[] extractJoinConditions(EvalNode joinQual, Schema leftSchema, Schema rightSchema) {
    List<EvalNode> equiQuals = Lists.newArrayList();
    List<EvalNode> joinQuals = Lists.newArrayList();
    List<EvalNode> leftFilters = Lists.newArrayList();
    List<EvalNode> rightFilters = Lists.newArrayList();
    EvalNode thetaQual = null;

    for (EvalNode eachQual : AlgebraicUtil.toConjunctiveNormalFormArray(joinQual)) {
      if (!(eachQual instanceof BinaryEval)) {
        continue; //todo 'between', etc.
      }
      BinaryEval binaryEval = (BinaryEval)eachQual;
      Set<Column> leftColumns = EvalTreeUtil.findUniqueColumns(binaryEval.getLeftExpr());
      Set<Column> rightColumns = EvalTreeUtil.findUniqueColumns(binaryEval.getRightExpr());
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
      if (eachQual.getType() == EvalType.EQUAL) {
        equiQuals.add(eachQual);
        continue;
      }
      if (leftInLeft) {
        binaryEval = AlgebraicUtil.commutate(binaryEval);
      }
      if (!isHashJoin() || thetaQual != null) {
        joinQuals.add(binaryEval);
      } else {
        thetaQual = binaryEval;
      }
    }
    if (thetaQual != null && !equiQuals.isEmpty()) {
      joinQuals.add(thetaQual);   // use equi-join
      thetaQual = null;
    }
    return new EvalNode[] {
        equiQuals.isEmpty() ? null : AlgebraicUtil.createSingletonExprFromCNF(equiQuals),
        joinQuals.isEmpty() ? null : AlgebraicUtil.createSingletonExprFromCNF(joinQuals),
        leftFilters.isEmpty() ? null : AlgebraicUtil.createSingletonExprFromCNF(leftFilters),
        rightFilters.isEmpty() ? null : AlgebraicUtil.createSingletonExprFromCNF(rightFilters),
        thetaQual
    };
  }

  public JoinNode getPlan() {
    return plan;
  }

  protected final boolean leftFiltered(Tuple left) {
    return leftPredicate != null && !leftPredicate.apply(left);
  }

  protected final boolean rightFiltered(Tuple right) {
    return rightPredicate != null && !rightPredicate.apply(right);
  }

  protected final Iterator<Tuple> filterLeft(Iterable<Tuple> iterable) {
    return filter(leftPredicate, iterable.iterator());
  }

  protected final Iterator<Tuple> filterRight(Iterable<Tuple> iterable) {
    return filter(rightPredicate, iterable.iterator());
  }

  protected final Iterator<Tuple> filter(Predicate<Tuple> predicate, Iterator<Tuple> tuples) {
    return predicate == null || !tuples.hasNext() ? tuples : Iterators.filter(tuples, predicate);
  }

  protected Predicate<Tuple> toPredicate(final EvalNode filter) {
    return filter == null ? null : new TuplePredicate(filter);
  }

  protected Predicate<Tuple> toFramePredicate(final EvalNode filter) {
    return filter == null ? null : new TuplePredicate(filter) {
      @Override
      public boolean apply(Tuple input) { return super.apply(frameTuple.setRight(input)); }
    };
  }

  protected final Predicate<Tuple> mergePredicates(Predicate<Tuple>... predicates) {
    Predicate<Tuple> current = null;
    for (Predicate<Tuple> predicate : predicates) {
      if (current == null) {
        current = predicate;
      } else if (predicate != null) {
        current = Predicates.and(current, predicate);
      }
    }
    return current;
  }

  private static class TuplePredicate implements Predicate<Tuple> {
    private final EvalNode filter;
    private TuplePredicate(EvalNode filter) { this.filter = filter; }
    public boolean apply(Tuple input) { return filter.eval(input).asBool(); }
  }

  protected Iterator<Tuple> nullIterator(int length) {
    return Arrays.asList(NullTuple.create(length)).iterator();
  }

  @Override
  protected void compile() {
    super.compile();
    if (joinQual != null) {
      joinQual = context.getPrecompiledEval(inSchema, joinQual);
    }
    // compile filters?
  }

  @Override
  public void init() throws IOException {
    super.init();
    if (equiQual != null) {
      equiQual.bind(context.getEvalContext(), inSchema);
    }
    if (joinQual != null) {
      joinQual.bind(context.getEvalContext(), inSchema);
    }
    if (thetaQual != null) {
      thetaQual.bind(context.getEvalContext(), inSchema);
    }
    if (leftFilter != null) {
      leftFilter.bind(context.getEvalContext(), leftSchema);
    }
    if (rightFilter != null) {
      rightFilter.bind(context.getEvalContext(), rightSchema);
    }
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();
    frameTuple.clear();
  }

  @Override
  public void close() throws IOException {
    super.close();
    frameTuple.clear();
    plan = null;
    equiQual = null;
    joinQual = null;
    thetaQual = null;
    leftFilter = null;
    rightFilter = null;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() +
        " [" + leftSchema.getAliases() + " : " + rightSchema.getAliases() + "]";
  }
}
