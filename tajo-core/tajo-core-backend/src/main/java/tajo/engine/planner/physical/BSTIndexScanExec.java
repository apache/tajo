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

import org.apache.hadoop.fs.Path;
import tajo.TaskAttemptContext;
import tajo.catalog.Schema;
import tajo.datum.Datum;
import tajo.engine.eval.EvalContext;
import tajo.engine.eval.EvalNode;
import tajo.engine.planner.Projector;
import tajo.engine.planner.logical.ScanNode;
import tajo.storage.*;
import tajo.storage.index.bst.BSTIndex;

import java.io.IOException;

public class BSTIndexScanExec extends PhysicalExec {
  private ScanNode scanNode;
  private SeekableScanner fileScanner;
  
  private EvalNode qual;
  private EvalContext qualCtx;
  private BSTIndex.BSTIndexReader reader;
  
  private final Projector projector;
  private EvalContext [] evalContexts;
  
  private Datum[] datum = null;
  
  private boolean initialize = true;
  
  public BSTIndexScanExec(TaskAttemptContext context,
                          StorageManager sm , ScanNode scanNode ,
       Fragment fragment, Path fileName , Schema keySchema,
       TupleComparator comparator , Datum[] datum) throws IOException {
    super(context, scanNode.getInSchema(), scanNode.getOutSchema());
    this.scanNode = scanNode;
    this.qual = scanNode.getQual();
    if(this.qual == null) {
      this.qualCtx = null;
    } else {
      this.qualCtx = this.qual.newContext();
    }
    this.datum = datum;

    this.fileScanner = (SeekableScanner)StorageManager.getScanner(context.getConf(),
        fragment.getMeta(), fragment, outSchema);
    this.projector = new Projector(inSchema, outSchema, scanNode.getTargets());
    this.evalContexts = projector.renew();

    this.reader = new BSTIndex(sm.getFileSystem().getConf()).
        getIndexReader(fileName, keySchema, comparator);
    this.reader.open();
  }

  @Override
  public void init() throws IOException {

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
    Tuple outTuple = new VTuple(this.outSchema.getColumnNum());
    if (!scanNode.hasQual()) {
      if ((tuple = fileScanner.next()) != null) {
        projector.eval(evalContexts, tuple);
        projector.terminate(evalContexts, outTuple);
        return outTuple;
      } else {
        return null;
      }
    } else {
       while( reader.isCurInMemory() && (tuple = fileScanner.next()) != null) {
         qual.eval(qualCtx, inSchema, tuple);
         if (qual.terminate(qualCtx).asBool()) {
           projector.eval(evalContexts, tuple);
           projector.terminate(evalContexts, outTuple);
           return outTuple;
         } else {
           fileScanner.seek(reader.next());
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
    reader.close();
    fileScanner.close();
  }

}
