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

import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;
import org.apache.tajo.worker.TaskAttemptContext;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.eval.EvalContext;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalTreeUtil;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.engine.planner.Target;
import org.apache.tajo.engine.planner.logical.ScanNode;
import org.apache.tajo.storage.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class SeqScanExec extends PhysicalExec {
  private final ScanNode plan;
  private Scanner scanner = null;

  private EvalNode qual = null;
  private EvalContext qualCtx;

  private CatalogProtos.FragmentProto [] fragments;

  private Projector projector;
  private EvalContext [] evalContexts;

  public SeqScanExec(TaskAttemptContext context, AbstractStorageManager sm,
                     ScanNode plan, CatalogProtos.FragmentProto [] fragments) throws IOException {
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
    Schema projected;
    if (plan.hasTargets()) {
      projected = new Schema();
      Set<Column> columnSet = new HashSet<Column>();

      if (plan.hasQual()) {
        columnSet.addAll(EvalTreeUtil.findDistinctRefColumns(qual));
      }

      for (Target t : plan.getTargets()) {
        columnSet.addAll(EvalTreeUtil.findDistinctRefColumns(t.getEvalTree()));
      }

      for (Column column : inSchema.getColumns()) {
        if (columnSet.contains(column)) {
          projected.addColumn(column);
        }
      }
    } else {
      projected = outSchema;
    }

    this.projector = new Projector(inSchema, outSchema, plan.getTargets());
    this.evalContexts = projector.renew();

    if (fragments.length > 1) {
      this.scanner = new MergeScanner(context.getConf(), plan.getTableSchema(), plan.getTableDesc().getMeta(),
          FragmentConvertor.<FileFragment>convert(context.getConf(), plan.getTableDesc().getMeta().getStoreType(),
              fragments), projected);
    } else {
      this.scanner = StorageManagerFactory.getStorageManager(
          context.getConf()).getScanner(plan.getTableDesc().getMeta(), plan.getTableSchema(), fragments[0], projected);
    }

    scanner.init();
  }

  @Override
  public Tuple next() throws IOException {
    Tuple tuple;
    Tuple outTuple = new VTuple(outColumnNum);

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
    return plan.getTableName();
  }
}
