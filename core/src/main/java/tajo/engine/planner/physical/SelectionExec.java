/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.engine.planner.physical;

import tajo.SubqueryContext;
import tajo.engine.eval.EvalContext;
import tajo.engine.eval.EvalNode;
import tajo.engine.planner.logical.SelectionNode;
import tajo.storage.RowStoreUtil;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.io.IOException;

/**
 * @author : hyunsik
 */
public class SelectionExec extends UnaryPhysicalExec  {
  private final EvalNode qual;
  private final EvalContext qualCtx;
  private final Tuple outputTuple;
  // projection
  private int [] targetIds;

  public SelectionExec(SubqueryContext context,
                       SelectionNode plan,
                       PhysicalExec child) {
    super(context, plan.getInSchema(), plan.getOutSchema(), child);

    this.qual = plan.getQual();
    this.qualCtx = this.qual.newContext();
    // for projection
    if (!inSchema.equals(outSchema)) {
      targetIds = RowStoreUtil.getTargetIds(inSchema, outSchema);
    }

    this.outputTuple = new VTuple(outSchema.getColumnNum());
  }

  @Override
  public Tuple next() throws IOException {
    Tuple tuple;
    while ((tuple = child.next()) != null) {
      qual.eval(qualCtx, inSchema, tuple);
      if (qual.terminate(qualCtx).asBool()) {
        if (targetIds != null) {
          RowStoreUtil.project(tuple, outputTuple, targetIds);
          return outputTuple;
        } else {
          return tuple;
        }
      }
    }

    return null;
  }
}
