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

import org.apache.tajo.catalog.partition.PartitionDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.utils.TupleUtil;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;
import org.apache.tajo.worker.TaskAttemptContext;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.engine.planner.Target;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.logical.ScanNode;
import org.apache.tajo.storage.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.tajo.catalog.proto.CatalogProtos.PartitionsType;

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

  /**
   * This method rewrites an input schema of column-partitioned table because
   * there are no actual field values in data file in a column-partitioned table.
   * So, this method removes partition key columns from the input schema.
   *
   * TODO - This implementation assumes that a fragment is always FileFragment.
   * In the column partitioned table, a path has an important role to
   * indicate partition keys. In this time, it is right. Later, we have to fix it.
   */
  private void rewriteColumnPartitionedTableSchema() throws IOException {
    PartitionDesc partitionDesc = plan.getTableDesc().getPartitions();
    Schema columnPartitionSchema = (Schema) partitionDesc.getSchema().clone();
    String qualifier = inSchema.getColumn(0).getQualifier();
    columnPartitionSchema.setQualifier(qualifier);

    // Remove partition key columns from an input schema.
    this.inSchema = PlannerUtil.rewriteColumnPartitionedTableSchema(
                                                 partitionDesc,
                                                 columnPartitionSchema,
                                                 inSchema,
                                                 qualifier);

    List<FileFragment> fileFragments = FragmentConvertor.convert(FileFragment.class, fragments);

    // Get a partition key value from a given path
    Tuple partitionRow =
        TupleUtil.buildTupleFromPartitionPath(columnPartitionSchema, fileFragments.get(0).getPath(), false);

    // Targets or search conditions may contain column references.
    // However, actual values absent in tuples. So, Replace all column references by constant datum.
    for (Column column : columnPartitionSchema.toArray()) {
      FieldEval targetExpr = new FieldEval(column);
      EvalContext evalContext = targetExpr.newContext();
      targetExpr.eval(evalContext, columnPartitionSchema, partitionRow);
      Datum datum = targetExpr.terminate(evalContext);
      ConstEval constExpr = new ConstEval(datum);
      for (Target target : plan.getTargets()) {
        if (target.getEvalTree().equals(targetExpr)) {
          if (!target.hasAlias()) {
            target.setAlias(target.getEvalTree().getName());
          }
          target.setExpr(constExpr);
        } else {
          EvalTreeUtil.replace(target.getEvalTree(), targetExpr, constExpr);
        }
      }

      if (plan.hasQual()) {
        EvalTreeUtil.replace(plan.getQual(), targetExpr, constExpr);
      }
    }
  }

  public void init() throws IOException {
    Schema projected;

    if (plan.getTableDesc().hasPartitions()
        && plan.getTableDesc().getPartitions().getPartitionsType() == PartitionsType.COLUMN) {
      rewriteColumnPartitionedTableSchema();
    }

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
        if (qual.terminate(qualCtx).isTrue()) {
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
