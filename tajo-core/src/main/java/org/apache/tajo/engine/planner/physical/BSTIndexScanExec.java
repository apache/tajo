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
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.engine.planner.logical.ScanNode;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.index.bst.BSTIndex;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;

public class BSTIndexScanExec extends PhysicalExec {
  private ScanNode scanNode;
  private SeekableScanner fileScanner;
  
  private EvalNode qual;
  private BSTIndex.BSTIndexReader reader;
  
  private Projector projector;
  
  private Datum[] datum = null;
  
  private boolean initialize = true;

  private float progress;

  public BSTIndexScanExec(TaskAttemptContext context,
                          AbstractStorageManager sm , ScanNode scanNode ,
       FileFragment fragment, Path fileName , Schema keySchema,
       TupleComparator comparator , Datum[] datum) throws IOException {
    super(context, scanNode.getInSchema(), scanNode.getOutSchema());
    this.scanNode = scanNode;
    this.qual = scanNode.getQual();
    this.datum = datum;

    this.fileScanner = StorageManagerFactory.getSeekableScanner(context.getConf(),
        scanNode.getTableDesc().getMeta(), scanNode.getInSchema(), fragment, outSchema);
    this.fileScanner.init();
    this.projector = new Projector(context, inSchema, outSchema, scanNode.getTargets());

    this.reader = new BSTIndex(sm.getFileSystem().getConf()).
        getIndexReader(fileName, keySchema, comparator);
    this.reader.open();
  }

  @Override
  public void init() throws IOException {
    progress = 0.0f;
  }

  @Override
  public Tuple next() throws IOException {
    if(initialize) {
      //TODO : more complicated condition
      Tuple key = new VTuple(datum.length);
      key.put(datum);
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
           long offset = reader.next();
           if (offset == -1) return null;
           else fileScanner.seek(offset);
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
