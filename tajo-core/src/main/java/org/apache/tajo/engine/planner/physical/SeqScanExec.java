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

import org.apache.hadoop.io.IOUtils;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.codegen.CompilationError;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.engine.utils.TupleCache;
import org.apache.tajo.engine.utils.TupleCacheKey;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.ConstEval;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.expr.EvalTreeUtil;
import org.apache.tajo.plan.expr.FieldEval;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.plan.rewrite.rules.PartitionedTableRewriter;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class SeqScanExec extends PhysicalExec {
  private ScanNode plan;

  private Scanner scanner = null;

  private EvalNode qual = null;

  private CatalogProtos.FragmentProto [] fragments;

  private Projector projector;

  private TableStats inputStats;

  private TupleCacheKey cacheKey;

  private boolean cacheRead = false;

  public SeqScanExec(TaskAttemptContext context, ScanNode plan,
                     CatalogProtos.FragmentProto [] fragments) throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema());

    this.plan = plan;
    this.qual = plan.getQual();
    this.fragments = fragments;

    if (plan.isBroadcastTable()) {
      String pathNameKey = "";
      if (fragments != null) {
        StringBuilder stringBuilder = new StringBuilder();
        for (FragmentProto f : fragments) {
          Fragment fragement = FragmentConvertor.convert(context.getConf(), f);
          stringBuilder.append(fragement.getKey());
        }
        pathNameKey = stringBuilder.toString();
      }

      cacheKey = new TupleCacheKey(
          context.getTaskId().getTaskId().getExecutionBlockId().toString(), plan.getTableName(), pathNameKey);
    }

    if (fragments != null
        && plan.getTableDesc().hasPartition()
        && plan.getTableDesc().getPartitionMethod().getPartitionType() == CatalogProtos.PartitionType.COLUMN) {
      rewriteColumnPartitionedTableSchema();
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
    PartitionMethodDesc partitionDesc = plan.getTableDesc().getPartitionMethod();
    Schema columnPartitionSchema = SchemaUtil.clone(partitionDesc.getExpressionSchema());
    String qualifier = inSchema.getColumn(0).getQualifier();
    columnPartitionSchema.setQualifier(qualifier);

    // Remove partition key columns from an input schema.
    this.inSchema = plan.getTableDesc().getSchema();

    List<FileFragment> fileFragments = FragmentConvertor.convert(FileFragment.class, fragments);

    // Get a partition key value from a given path
    Tuple partitionRow =
        PartitionedTableRewriter.buildTupleFromPartitionPath(columnPartitionSchema, fileFragments.get(0).getPath(),
            false);

    // Targets or search conditions may contain column references.
    // However, actual values absent in tuples. So, Replace all column references by constant datum.
    for (Column column : columnPartitionSchema.toArray()) {
      FieldEval targetExpr = new FieldEval(column);
      Datum datum = targetExpr.eval(columnPartitionSchema, partitionRow);
      ConstEval constExpr = new ConstEval(datum);


      for (int i = 0; i < plan.getTargets().length; i++) {
        Target target = plan.getTargets()[i];

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

    if (plan.hasTargets()) {
      projected = new Schema();
      Set<Column> columnSet = new HashSet<Column>();

      if (plan.hasQual()) {
        columnSet.addAll(EvalTreeUtil.findUniqueColumns(qual));
      }

      for (Target t : plan.getTargets()) {
        columnSet.addAll(EvalTreeUtil.findUniqueColumns(t.getEvalTree()));
      }

      for (Column column : inSchema.getColumns()) {
        if (columnSet.contains(column)) {
          projected.addColumn(column);
        }
      }
    } else {
      projected = outSchema;
    }

    if (cacheKey != null) {
      TupleCache tupleCache = TupleCache.getInstance();
      if (tupleCache.isBroadcastCacheReady(cacheKey)) {
        openCacheScanner();
      } else {
        if (TupleCache.getInstance().lockBroadcastScan(cacheKey)) {
          scanAndAddCache(projected);
          openCacheScanner();
        } else {
          Object lockMonitor = tupleCache.getLockMonitor();
          synchronized (lockMonitor) {
            try {
              lockMonitor.wait(20 * 1000);
            } catch (InterruptedException e) {
            }
          }
          if (tupleCache.isBroadcastCacheReady(cacheKey)) {
            openCacheScanner();
          } else {
            initScanner(projected);
          }
        }
      }
    } else {
      initScanner(projected);
    }

    super.init();
  }

  @Override
  protected void compile() throws CompilationError {
    if (plan.hasQual()) {
      qual = context.getPrecompiledEval(inSchema, qual);
    }
  }

  private void initScanner(Schema projected) throws IOException {
    this.projector = new Projector(context, inSchema, outSchema, plan.getTargets());
    TableMeta meta = null;
    try {
      meta = (TableMeta) plan.getTableDesc().getMeta().clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }

    // set system default properties
    PlannerUtil.applySystemDefaultToTableProperties(context.getQueryContext(), meta);

    if (fragments != null) {
      if (fragments.length > 1) {
        this.scanner = new MergeScanner(context.getConf(), plan.getPhysicalSchema(), meta,
            FragmentConvertor.convert(context.getConf(), fragments), projected
        );
      } else {
        StorageManager storageManager = StorageManager.getStorageManager(
            context.getConf(), plan.getTableDesc().getMeta().getStoreType());
        this.scanner = storageManager.getScanner(meta,
            plan.getPhysicalSchema(), fragments[0], projected);
      }
      scanner.init();
    }
  }

  private void openCacheScanner() throws IOException {
    Scanner cacheScanner = TupleCache.getInstance().openCacheScanner(cacheKey, plan.getPhysicalSchema());
    if (cacheScanner != null) {
      scanner = cacheScanner;
      cacheRead = true;
    }
  }

  private void scanAndAddCache(Schema projected) throws IOException {
    initScanner(projected);

    List<Tuple> broadcastTupleCacheList = new ArrayList<Tuple>();
    while (!context.isStopped()) {
      Tuple tuple = next();
      if (tuple != null) {
        broadcastTupleCacheList.add(tuple);
      } else {
        break;
      }
    }

    if (scanner != null) {
      scanner.close();
      scanner = null;
    }

    TupleCache.getInstance().addBroadcastCache(cacheKey, broadcastTupleCacheList);
  }

  @Override
  public Tuple next() throws IOException {
    if (fragments == null) {
      return null;
    }

    Tuple tuple;
    Tuple outTuple = new VTuple(outColumnNum);

    if (!plan.hasQual()) {
      if ((tuple = scanner.next()) != null) {
        if (cacheRead) {
          return tuple;
        }
        projector.eval(tuple, outTuple);
        outTuple.setOffset(tuple.getOffset());
        return outTuple;
      } else {
        return null;
      }
    } else {
      while ((tuple = scanner.next()) != null) {
        if (cacheRead) {
          return tuple;
        }
        if (qual.eval(inSchema, tuple).isTrue()) {
          projector.eval(tuple, outTuple);
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
    IOUtils.cleanup(null, scanner);
    if (scanner != null) {
      try {
        TableStats stat = scanner.getInputStats();
        if (stat != null) {
          inputStats = (TableStats)(stat.clone());
        }
      } catch (CloneNotSupportedException e) {
        e.printStackTrace();
      }
    }
    scanner = null;
    plan = null;
    qual = null;
    projector = null;
  }

  public String getTableName() {
    return plan.getTableName();
  }

  @Override
  public float getProgress() {
    if (scanner == null) {
      return 1.0f;
    } else {
      return scanner.getProgress();
    }
  }

  @Override
  public TableStats getInputStats() {
    if (scanner != null) {
      return scanner.getInputStats();
    } else {
      if (inputStats != null) {
        return inputStats;
      } else {
        // If no fragment, there is no scanner. So, we need to create a dummy table stat.
        return new TableStats();
      }
    }
  }

  @Override
  public String toString() {
    if (scanner != null) {
      return "SeqScanExec:" + plan + "," + scanner.getClass().getName();
    } else {
      return "SeqScanExec:" + plan;
    }
  }
}
