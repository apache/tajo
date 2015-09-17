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
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.codegen.CompilationError;
import org.apache.tajo.engine.planner.Projector;
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
import org.apache.tajo.storage.fragment.FragmentConvertor;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class SeqScanExec extends ScanExec {
  private ScanNode plan;

  private Scanner scanner = null;

  private EvalNode qual = null;

  private CatalogProtos.FragmentProto [] fragments;

  private Projector projector;

  private TableStats inputStats;

  // scanner iterator with filter or without filter
  private ScanIterator scanIt;

  private boolean needProjection;

  public SeqScanExec(TaskAttemptContext context, ScanNode plan,
                     CatalogProtos.FragmentProto [] fragments) throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema());

    this.plan = plan;
    this.qual = plan.getQual();
    this.fragments = fragments;

    if (plan.getTableDesc().hasPartition() &&
        plan.getTableDesc().getPartitionMethod().getPartitionType() == CatalogProtos.PartitionType.COLUMN) {
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

    Tuple partitionRow = null;
    if (fragments != null && fragments.length > 0) {
      List<FileFragment> fileFragments = FragmentConvertor.convert(FileFragment.class, fragments);

      // Get a partition key value from a given path
      partitionRow = PartitionedTableRewriter.buildTupleFromPartitionPath(
              columnPartitionSchema, fileFragments.get(0).getPath(), false);
    }

    // Targets or search conditions may contain column references.
    // However, actual values absent in tuples. So, Replace all column references by constant datum.
    for (Column column : columnPartitionSchema.toArray()) {
      FieldEval targetExpr = new FieldEval(column);
      Datum datum = NullDatum.get();
      if (partitionRow != null) {
        targetExpr.bind(context.getEvalContext(), columnPartitionSchema);
        datum = targetExpr.eval(partitionRow);
      }
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

  public Schema getProjectSchema() {
    Schema projected;

    // in the case where projected column or expression are given
    // the target can be an empty list.
    if (plan.hasTargets()) {
      projected = new Schema();
      Set<Column> columnSet = new HashSet<Column>();

      if (plan.hasQual()) {
        columnSet.addAll(EvalTreeUtil.findUniqueColumns(qual));
      }

      for (Target t : plan.getTargets()) {
        columnSet.addAll(EvalTreeUtil.findUniqueColumns(t.getEvalTree()));
      }

      for (Column column : inSchema.getAllColumns()) {
        if (columnSet.contains(column)) {
          projected.addColumn(column);
        }
      }

    } else {
      // no any projected columns, meaning that all columns should be projected.
      // TODO - this implicit rule makes code readability bad. So, we should remove it later
      projected = outSchema;
    }

    return projected;
  }

  private void initScanIterator() {
    // We should use FilterScanIterator only if underlying storage does not support filter push down.
    if (plan.hasQual() && !scanner.isSelectable()) {
      scanIt = new FilterScanIterator(scanner, qual);

    } else {
      scanIt = new FullScanIterator(scanner);
    }
  }

  @Override
  public void init() throws IOException {

    // Why we should check nullity? See https://issues.apache.org/jira/browse/TAJO-1422

    if (fragments == null) {
      scanIt = new EmptyScanIterator();

    } else {
      Schema projectedFields = getProjectSchema();
      initScanner(projectedFields);

      // See Scanner.isProjectable() method. Depending on the result of isProjectable(),
      // the width of retrieved tuple is changed.
      //
      // If projectable, the retrieved tuple will contain only projected fields.
      // Otherwise, the retrieved tuple will contain projected fields and NullDatum
      // for non-projected fields.
      Schema actualInSchema = scanner.isProjectable() ? projectedFields : inSchema;

      initializeProjector(actualInSchema);

      if (plan.hasQual()) {
        qual.bind(context.getEvalContext(), actualInSchema);
      }

      initScanIterator();
    }

    super.init();
  }

  protected void initializeProjector(Schema actualInSchema){
    Target[] realTargets;
    if (plan.getTargets() == null) {
      realTargets = PlannerUtil.schemaToTargets(outSchema);
    } else {
      realTargets = plan.getTargets();
    }

    //if all column is selected and there is no have expression, projection can be skipped
    if (realTargets.length == inSchema.size()) {
      for (int i = 0; i < inSchema.size(); i++) {
        if (realTargets[i].getEvalTree() instanceof FieldEval) {
          FieldEval f = realTargets[i].getEvalTree();
          if(!f.getColumnRef().equals(inSchema.getColumn(i))) {
            needProjection = true;
            break;
          }
        } else {
          needProjection = true;
          break;
        }
      }
    } else {
      needProjection = true;
    }

    if(needProjection) {
      projector = new Projector(context, actualInSchema, outSchema, plan.getTargets());
    }
  }

  @Override
  public ScanNode getScanNode() {
    return plan;
  }

  @Override
  protected void compile() throws CompilationError {
    if (plan.hasQual()) {
      qual = context.getPrecompiledEval(inSchema, qual);
    }
  }

  private void initScanner(Schema projected) throws IOException {
    TableDesc table = plan.getTableDesc();
    TableMeta meta = table.getMeta();

    if (fragments.length > 1) {

      this.scanner = new MergeScanner(
          context.getConf(),
          plan.getPhysicalSchema(), meta,
          FragmentConvertor.convert(context.getConf(), fragments),
          projected
      );

    } else {

      Tablespace tablespace = TablespaceManager.get(table.getUri()).get();
      this.scanner = tablespace.getScanner(
          meta,
          plan.getPhysicalSchema(),
          FragmentConvertor.convert(context.getConf(), fragments[0]),
          projected);
    }

    if (scanner.isSelectable()) { // TODO - isSelectable should be moved to FormatProperty
      scanner.setFilter(qual);
    }

    if (plan.hasLimit()) {
      scanner.setLimit(plan.getLimit());
    }
    scanner.init();
  }

  @Override
  public Tuple next() throws IOException {

    while(scanIt.hasNext()) {
      Tuple t = scanIt.next();
      if(!needProjection) return t;

      Tuple outTuple = projector.eval(t);
      outTuple.setOffset(t.getOffset());
      return outTuple;
    }

    return null;
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
  }

  @Override
  public String getTableName() {
    return plan.getTableName();
  }

  @Override
  public String getCanonicalName() {
    return plan.getCanonicalName();
  }

  @Override
  public CatalogProtos.FragmentProto[] getFragments() {
    return fragments;
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
