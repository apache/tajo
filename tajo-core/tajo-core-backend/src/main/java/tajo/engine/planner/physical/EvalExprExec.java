/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * 
 */
package tajo.engine.planner.physical;

import tajo.TaskAttemptContext;
import tajo.engine.eval.EvalContext;
import tajo.engine.parser.QueryBlock.Target;
import tajo.engine.planner.logical.EvalExprNode;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.io.IOException;

/**
 * @author Hyunsik Choi
 */
public class EvalExprExec extends PhysicalExec {
  private final EvalExprNode plan;
  private final EvalContext[] evalContexts;
  
  /**
   * 
   */
  public EvalExprExec(final TaskAttemptContext context, final EvalExprNode plan) {
    super(context, plan.getInSchema(), plan.getOutSchema());
    this.plan = plan;

    evalContexts = new EvalContext[plan.getExprs().length];
    for (int i = 0; i < plan.getExprs().length; i++) {
      evalContexts[i] = plan.getExprs()[i].getEvalTree().newContext();
    }
  }

  @Override
  public void init() throws IOException {
  }

  /* (non-Javadoc)
  * @see PhysicalExec#next()
  */
  @Override
  public Tuple next() throws IOException {    
    Target [] targets = plan.getExprs();
    Tuple t = new VTuple(targets.length);
    for (int i = 0; i < targets.length; i++) {
      targets[i].getEvalTree().eval(evalContexts[i], inSchema, null);
      t.put(i, targets[i].getEvalTree().terminate(evalContexts[i]));
    }
    return t;
  }

  @Override
  public void rescan() throws IOException {    
  }

  @Override
  public void close() throws IOException {
  }
}
