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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.expr.EvalTreeUtil;
import org.apache.tajo.plan.logical.IndexScanNode;
import org.apache.tajo.plan.rewrite.rules.IndexScanInfo.SimplePredicate;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.index.bst.BSTIndex;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.net.URI;
import java.util.Set;

public class BSTIndexScanExec extends PhysicalExec {
  private final static Log LOG = LogFactory.getLog(BSTIndexScanExec.class);
  private IndexScanNode scanNode;
  private SeekableScanner fileScanner;
  
  private EvalNode qual;
  private BSTIndex.BSTIndexReader reader;
  
  private Projector projector;
  
  private Datum[] values = null;

  private boolean initialize = true;

  private float progress;

  private TableStats inputStats;

  public BSTIndexScanExec(TaskAttemptContext context,
                          IndexScanNode scanNode ,
       FileFragment fragment, URI indexPrefix , Schema keySchema,
       SimplePredicate [] predicates) throws IOException {
    super(context, scanNode.getInSchema(), scanNode.getOutSchema());
    this.scanNode = scanNode;
    this.qual = scanNode.getQual();

    SortSpec[] keySortSpecs = new SortSpec[predicates.length];
    values = new Datum[predicates.length];
    for (int i = 0; i < predicates.length; i++) {
      keySortSpecs[i] = predicates[i].getKeySortSpec();
      values[i] = predicates[i].getValue();
    }

    TupleComparator comparator = new BaseTupleComparator(keySchema,
        keySortSpecs);

    Schema fileScanOutSchema = mergeSubSchemas(inSchema, keySchema, scanNode.getTargets(), qual);

    this.fileScanner = StorageManager.getSeekableScanner(context.getConf(),
        scanNode.getTableDesc().getMeta(), inSchema, fragment, fileScanOutSchema);
    this.fileScanner.init();
    this.projector = new Projector(context, inSchema, outSchema, scanNode.getTargets());

    Path indexPath = new Path(indexPrefix.toString(), context.getUniqueKeyFromFragments());
    this.reader = new BSTIndex(context.getConf()).
        getIndexReader(indexPath, keySchema, comparator);
    this.reader.open();
  }

  private static Schema mergeSubSchemas(Schema originalSchema, Schema subSchema, Target[] targets, EvalNode qual) {
    Schema mergedSchema = new Schema();
    Set<Column> qualAndTargets = TUtil.newHashSet();
    qualAndTargets.addAll(EvalTreeUtil.findUniqueColumns(qual));
    for (Target target : targets) {
      qualAndTargets.addAll(EvalTreeUtil.findUniqueColumns(target.getEvalTree()));
    }
    for (Column column : originalSchema.getColumns()) {
      if (subSchema.contains(column)
          || qualAndTargets.contains(column)
          || qualAndTargets.contains(column)) {
        mergedSchema.addColumn(column);
      }
    }
    return mergedSchema;
  }

  @Override
  public void init() throws IOException {
    super.init();
    progress = 0.0f;
    if (qual != null) {
      qual.bind(inSchema);
    }
  }

  @Override
  public Tuple next() throws IOException {
    if(initialize) {
      //TODO : more complicated condition
      Tuple key = new VTuple(values.length);
      key.put(values);
      long offset = reader.find(key);
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
    Tuple outTuple = new VTuple(this.outSchema.size());
    if (!scanNode.hasQual()) {
      if ((tuple = fileScanner.next()) != null) {
        projector.eval(tuple, outTuple);
        return outTuple;
      } else {
        return null;
      }
    } else {
       while(reader.isCurInMemory() && (tuple = fileScanner.next()) != null) {
         if (qual.eval(tuple).isTrue()) {
           projector.eval(tuple, outTuple);
           return outTuple;
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
    scanNode = null;
    qual = null;
    projector = null;
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
