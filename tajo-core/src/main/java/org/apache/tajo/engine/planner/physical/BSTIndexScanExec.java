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
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.IndexScanNode;
import org.apache.tajo.plan.rewrite.rules.IndexScanInfo.SimplePredicate;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.index.bst.BSTIndex;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

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

  public BSTIndexScanExec(TaskAttemptContext context,
                          StorageManager sm , IndexScanNode scanNode ,
       FileFragment fragment, Path indexPrefix , Schema keySchema,
       SimplePredicate [] predicates) throws IOException {
    super(context, scanNode.getInSchema(), scanNode.getOutSchema());
    this.scanNode = scanNode;
    this.qual = scanNode.getQual();

    SortSpec[] keySortSpecs = new SortSpec[predicates.length];
    values = new Datum[predicates.length];
    for (int i = 0; i < predicates.length; i++) {
      keySortSpecs[i] = predicates[i].getSortSpec();
      values[i] = predicates[i].getValue();
    }

    TupleComparator comparator = new BaseTupleComparator(keySchema,
        keySortSpecs);

    Schema fileOutSchema = mergeSubSchemas(inSchema, keySchema, outSchema);

    this.fileScanner = StorageManager.getSeekableScanner(context.getConf(),
        scanNode.getTableDesc().getMeta(), inSchema, fragment, fileOutSchema);
    this.fileScanner.init();
    this.projector = new Projector(context, inSchema, outSchema, scanNode.getTargets());

    Path indexPath = new Path(indexPrefix, context.getUniqueKeyFromFragments());
    this.reader = new BSTIndex(sm.getFileSystem().getConf()).
        getIndexReader(indexPath, keySchema, comparator);
    this.reader.open();
  }

  private static Schema mergeSubSchemas(Schema originalSchema, Schema subSchema1, Schema subSchema2) {
    int i1 = 0, i2 = 0;
    Schema mergedSchema = new Schema();
    while (i1 < subSchema1.size() && i2 < subSchema2.size()) {
      int columnId1 = originalSchema.getColumnId(subSchema1.getColumn(i1).getQualifiedName());
      int columnId2 = originalSchema.getColumnId(subSchema2.getColumn(i2).getQualifiedName());
      if (columnId1 < columnId2) {
        mergedSchema.addColumn(originalSchema.getColumn(columnId1));
        i1++;
      } else if (columnId1 > columnId2) {
        mergedSchema.addColumn(originalSchema.getColumn(columnId2));
        i2++;
      } else {
        mergedSchema.addColumn(originalSchema.getColumn(columnId1));
        i1++;
        i2++;
      }
    }
    for (; i1 < subSchema1.size(); i1++) {
      mergedSchema.addColumn(subSchema1.getColumn(i1));
    }
    for (; i2 < subSchema2.size(); i2++) {
      mergedSchema.addColumn(subSchema2.getColumn(i2));
    }
    return mergedSchema;
  }

  @Override
  public void init() throws IOException {
    progress = 0.0f;
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
         if (qual.eval(inSchema, tuple).isTrue()) {
           projector.eval(tuple, outTuple);
           return outTuple;
         } else {
//           long offset = reader.next();
//           if (offset == -1) {
//             return null;
//           }
//           else fileScanner.seek(offset);
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
}
