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

import com.google.common.collect.Sets;
import org.apache.tajo.worker.TaskAttemptContext;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.eval.ConstEval;
import org.apache.tajo.engine.eval.EvalContext;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalType;
import org.apache.tajo.engine.planner.Target;
import org.apache.tajo.engine.planner.logical.GroupbyNode;

import java.io.IOException;
import java.util.Set;

public abstract class AggregationExec extends UnaryPhysicalExec {
  protected GroupbyNode plan;

  protected Set<Column> nonNullGroupingFields;
  protected int keylist [];
  protected int measureList[];
  protected final EvalNode evals [];
  protected EvalContext evalContexts [];
  protected Schema evalSchema;

  protected EvalNode havingQual;
  protected EvalContext havingContext;

  public AggregationExec(final TaskAttemptContext context, GroupbyNode plan,
                         PhysicalExec child) throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema(), child);
    this.plan = plan;

    if (plan.hasHavingCondition()) {
      this.havingQual = plan.getHavingCondition();
      this.havingContext = plan.getHavingCondition().newContext();
    }

    if (plan.getHavingSchema() != null) {
      this.evalSchema = plan.getHavingSchema();
    } else {
      this.evalSchema = plan.getOutSchema();
    }

    nonNullGroupingFields = Sets.newHashSet();
    // keylist will contain a list of IDs of grouping column
    keylist = new int[plan.getGroupingColumns().length];
    Column col;
    for (int idx = 0; idx < plan.getGroupingColumns().length; idx++) {
      col = plan.getGroupingColumns()[idx];
      keylist[idx] = inSchema.getColumnId(col.getQualifiedName());
      nonNullGroupingFields.add(col);
    }

    // measureList will contain a list of IDs of measure fields
    int valueIdx = 0;
    measureList = new int[plan.getTargets().length - keylist.length];
    if (measureList.length > 0) {
      search: for (int inputIdx = 0; inputIdx < plan.getTargets().length; inputIdx++) {
        for (int key : keylist) { // eliminate key field
          if (plan.getTargets()[inputIdx].getColumnSchema().getColumnName()
              .equals(inSchema.getColumn(key).getColumnName())) {
            continue search;
          }
        }
        measureList[valueIdx] = inputIdx;
        valueIdx++;
      }
    }

    evals = new EvalNode[plan.getTargets().length];
    evalContexts = new EvalContext[plan.getTargets().length];
    for (int i = 0; i < plan.getTargets().length; i++) {
      Target t = plan.getTargets()[i];
      if (t.getEvalTree().getType() == EvalType.FIELD && !nonNullGroupingFields.contains(t.getColumnSchema())) {
        evals[i] = new ConstEval(DatumFactory.createNullDatum());
        evalContexts[i] = evals[i].newContext();
      } else {
        evals[i] = t.getEvalTree();
        evalContexts[i] = evals[i].newContext();
      }
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    nonNullGroupingFields.clear();
  }
}
