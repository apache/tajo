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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.expr.EvalTreeUtil;
import org.apache.tajo.plan.logical.IndexScanNode;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.plan.rewrite.rules.IndexScanInfo.SimplePredicate;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.index.bst.BSTIndex;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BSTIndexScanExec extends ScanExec {
  private IndexScanNode plan;
  private SeekableScanner fileScanner;
  
  private EvalNode qual;
  private BSTIndex.BSTIndexReader reader;
  
  private Projector projector;

  private boolean initialize = true;

  private float progress;

  private Tuple indexLookupKey;

  private TableStats inputStats;

  private CatalogProtos.FragmentProto fragment;

  private Schema keySchema;

  public BSTIndexScanExec(TaskAttemptContext context, IndexScanNode plan,
                          CatalogProtos.FragmentProto fragment, URI indexPrefix , Schema keySchema,
                          SimplePredicate [] predicates) throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema());
    this.plan = plan;
    this.qual = plan.getQual();
    this.fragment = fragment;
    this.keySchema = keySchema;

    SortSpec[] keySortSpecs = new SortSpec[predicates.length];
    Datum[] values = new Datum[predicates.length];
    for (int i = 0; i < predicates.length; i++) {
      keySortSpecs[i] = predicates[i].getKeySortSpec();
      values[i] = predicates[i].getValue();
    }
    indexLookupKey = new VTuple(values);

    TupleComparator comparator = new BaseTupleComparator(keySchema,
        keySortSpecs);

    this.projector = new Projector(context, inSchema, outSchema, plan.getTargets());

    Path indexPath = new Path(indexPrefix.toString(), IndexExecutorUtil.getIndexFileName(fragment));
    this.reader = new BSTIndex(context.getConf()).
        getIndexReader(indexPath, keySchema, comparator);
  }

  private static Schema mergeSubSchemas(Schema originalSchema, Schema subSchema, List<Target> targets, EvalNode qual) {
    Schema mergedSchema = new Schema();
    Set<Column> qualAndTargets = new HashSet<>();
    qualAndTargets.addAll(EvalTreeUtil.findUniqueColumns(qual));
    for (Target target : targets) {
      qualAndTargets.addAll(EvalTreeUtil.findUniqueColumns(target.getEvalTree()));
    }
    for (Column column : originalSchema.getRootColumns()) {
      if (subSchema.contains(column) || qualAndTargets.contains(column)) {
        mergedSchema.addColumn(column);
      }
    }
    return mergedSchema;
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
  public FragmentProto[] getFragments() {
    return new FragmentProto[]{fragment};
  }

  @Override
  public void init() throws IOException {
    reader.init();

    Schema projected;

    // in the case where projected column or expression are given
    // the target can be an empty list.
    if (plan.hasTargets()) {
      projected = new Schema();
      Set<Column> columnSet = new HashSet<>();

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

    initScanner(projected);
    super.init();
    progress = 0.0f;

    if (plan.hasQual()) {
      if (fileScanner.isProjectable()) {
        qual.bind(context.getEvalContext(), projected);
      } else {
        qual.bind(context.getEvalContext(), inSchema);
      }
    }
  }

  @Override
  public ScanNode getScanNode() {
    return plan;
  }

  private void initScanner(Schema projected) throws IOException {

    // Why we should check nullity? See https://issues.apache.org/jira/browse/TAJO-1422
    if (fragment != null) {

      Schema fileScanOutSchema = mergeSubSchemas(projected, keySchema, plan.getTargets(), qual);

      this.fileScanner = OldStorageManager.getStorageManager(context.getConf(),
          plan.getTableDesc().getMeta().getDataFormat())
          .getSeekableScanner(plan.getTableDesc().getMeta(), plan.getPhysicalSchema(), fragment, fileScanOutSchema);
      this.fileScanner.init();

      // See Scanner.isProjectable() method Depending on the result of isProjectable(),
      // the width of retrieved tuple is changed.
      //
      // If TRUE, the retrieved tuple will contain only projected fields.
      // If FALSE, the retrieved tuple will contain projected fields and NullDatum for non-projected fields.
      if (fileScanner.isProjectable()) {
        this.projector = new Projector(context, projected, outSchema, plan.getTargets());
      } else {
        this.projector = new Projector(context, inSchema, outSchema, plan.getTargets());
      }
    }
  }

  @Override
  public Tuple next() throws IOException {
    if(initialize) {
      //TODO : more complicated condition
      long offset = reader.find(indexLookupKey);
      if (offset == -1) {
        reader.close();
        fileScanner.close();
        return null;
      }else {
        fileScanner.seek(offset);
      }
      initialize = false;
    } else {
      if(!reader.isCurInMemory()) {
        return null;
      }
      long offset = reader.next();
      if(offset == -1 ) {
        reader.close();
        fileScanner.close();
        return null;
      } else { 
      fileScanner.seek(offset);
      }
    }

    Tuple tuple;
    if (!plan.hasQual()) {
      if ((tuple = fileScanner.next()) != null) {
        return projector.eval(tuple);
      } else {
        return null;
      }
    } else {
       while(reader.isCurInMemory() && (tuple = fileScanner.next()) != null) {
         if (qual.eval(tuple).isTrue()) {
           return projector.eval(tuple);
         } else {
           long offset = reader.next();
           if (offset == -1) {
             return null;
           }
           else fileScanner.seek(offset);
           return null;
         }
       }
     }

    return null;
  }
  @Override
  public void rescan() throws IOException {
    fileScanner.reset();
  }

  @Override
  public void close() throws IOException {
    IOUtils.cleanup(null, reader, fileScanner);
    if (fileScanner != null) {
      try {
        TableStats stats = fileScanner.getInputStats();
        if (stats != null) {
          inputStats = (TableStats) stats.clone();
        }
      } catch (CloneNotSupportedException e) {
        e.printStackTrace();
      }
    }
    reader = null;
    fileScanner = null;
    plan = null;
    qual = null;
    projector = null;
    indexLookupKey = null;
  }

  @Override
  public float getProgress() {
    return progress;
  }

  @Override
  public TableStats getInputStats() {
    if (fileScanner != null) {
      return fileScanner.getInputStats();
    } else {
      return inputStats;
    }
  }
}
