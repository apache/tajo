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

import org.apache.tajo.catalog.Column;
import org.apache.tajo.plan.expr.AggregationFunctionCallEval;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.GroupbyNode;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class AggregationExec extends UnaryPhysicalExec {

  protected final int groupingKeyNum;
  protected final int aggFunctionsNum;
  protected final List<AggregationFunctionCallEval> aggFunctions;

  public AggregationExec(final TaskAttemptContext context, GroupbyNode plan,
                         PhysicalExec child) throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema(), child);

    final Column [] keyColumns = plan.getGroupingColumns();
    groupingKeyNum = keyColumns.length;

    if (plan.hasAggFunctions()) {
      aggFunctions = plan.getAggFunctions();
      aggFunctionsNum = aggFunctions.size();
    } else {
      aggFunctions = new ArrayList<>();
      aggFunctionsNum = 0;
    }
  }

  @Override
  public void init() throws IOException {
    super.init();
    for (EvalNode aggFunction : aggFunctions) {
      aggFunction.bind(context.getEvalContext(), inSchema);
    }
  }
}
