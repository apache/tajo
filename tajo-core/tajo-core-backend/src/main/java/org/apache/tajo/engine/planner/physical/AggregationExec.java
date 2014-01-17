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
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.eval.ConstEval;
import org.apache.tajo.engine.eval.EvalContext;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalType;
import org.apache.tajo.engine.planner.Target;
import org.apache.tajo.engine.planner.logical.GroupbyNode;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public abstract class AggregationExec extends UnaryPhysicalExec {
  protected GroupbyNode plan;

  protected Set<Column> nonNullGroupingFields;
  protected int keylist [];
  protected int measureList[];
  protected final EvalNode evals [];
  protected EvalContext evalContexts [];
  protected Schema evalSchema;

  public AggregationExec(final TaskAttemptContext context, GroupbyNode plan,
                         PhysicalExec child) throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema(), child);
    this.plan = plan;

    evalSchema = plan.getOutSchema();

    nonNullGroupingFields = Sets.newHashSet();
    // keylist will contain a list of IDs of grouping column
    keylist = new int[plan.getGroupingColumns().length];
    Column col;
    for (int idx = 0; idx < plan.getGroupingColumns().length; idx++) {
      col = plan.getGroupingColumns()[idx];
      keylist[idx] = inSchema.getColumnId(col.getQualifiedName());
      nonNullGroupingFields.add(col);
    }

    // measureList will contain a list of measure field indexes against the target list.
    List<Integer> measureIndexes = TUtil.newList();
    for (int i = 0; i < plan.getTargets().length; i++) {
      Target target = plan.getTargets()[i];
      if (target.getEvalTree().getType() == EvalType.AGG_FUNCTION) {
        measureIndexes.add(i);
      }
    }

    measureList = new int[measureIndexes.size()];
    for (int i = 0; i < measureIndexes.size(); i++) {
      measureList[i] = measureIndexes.get(i);
    }

    evals = new EvalNode[plan.getTargets().length];
    evalContexts = new EvalContext[plan.getTargets().length];
    for (int i = 0; i < plan.getTargets().length; i++) {
      Target t = plan.getTargets()[i];
      if (t.getEvalTree().getType() == EvalType.FIELD && !nonNullGroupingFields.contains(t.getNamedColumn())) {
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
