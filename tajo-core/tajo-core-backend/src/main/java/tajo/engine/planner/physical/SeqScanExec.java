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

package tajo.engine.planner.physical;

import tajo.TaskAttemptContext;
import tajo.engine.eval.EvalContext;
import tajo.engine.eval.EvalNode;
import tajo.engine.planner.Projector;
import tajo.engine.planner.logical.ScanNode;
import tajo.storage.*;
import tajo.util.TUtil;

import java.io.IOException;

public class SeqScanExec extends PhysicalExec {
  private final ScanNode plan;
  private Scanner scanner = null;

  private EvalNode qual = null;
  private EvalContext qualCtx;

  private Fragment [] fragments;

  private Projector projector;
  private EvalContext [] evalContexts;

  public SeqScanExec(TaskAttemptContext context, StorageManager sm,
                     ScanNode plan, Fragment[] fragments) throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema());

    this.plan = plan;
    this.qual = plan.getQual();
    this.fragments = fragments;

    if (qual == null) {
      qualCtx = null;
    } else {
      qualCtx = this.qual.newContext();
    }
  }

  public void init() throws IOException {
    this.projector = new Projector(inSchema, outSchema, plan.getTargets());
    this.evalContexts = projector.renew();

    if (fragments.length > 1) {
      this.scanner = new MergeScanner(context.getConf(), fragments[0].getMeta(),
          TUtil.newList(fragments));
    } else {
      this.scanner = StorageManager.getScanner(context.getConf(), fragments[0].getMeta(),
          fragments[0], plan.getOutSchema());
    }
  }

  @Override
  public Tuple next() throws IOException {
    Tuple tuple;
    Tuple outTuple = new VTuple(outSchema.getColumnNum());

    if (!plan.hasQual()) {
      if ((tuple = scanner.next()) != null) {
        projector.eval(evalContexts, tuple);
        projector.terminate(evalContexts, outTuple);
        outTuple.setOffset(tuple.getOffset());
        return outTuple;
      } else {
        return null;
      }
    } else {
      while ((tuple = scanner.next()) != null) {
        qual.eval(qualCtx, inSchema, tuple);
        if (qual.terminate(qualCtx).asBool()) {
          projector.eval(evalContexts, tuple);
          projector.terminate(evalContexts, outTuple);
          return outTuple;
        }
      }
      return null;
    }
  }

  @Override
  public void rescan() throws IOException {
    scanner.reset();
  }

  @Override
  public void close() throws IOException {
    scanner.close();
  }

  public String getTableName() {
    return plan.getTableId();
  }
}
