package nta.engine.planner.physical;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

import tajo.index.bst.BSTIndex;

import nta.catalog.Schema;
import nta.datum.Datum;
import nta.engine.exec.eval.EvalContext;
import nta.engine.exec.eval.EvalNode;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.planner.Projector;
import nta.engine.planner.logical.ScanNode;
import nta.storage.SeekableScanner;
import nta.storage.StorageManager;
import nta.storage.Tuple;
import nta.storage.VTuple;

public class BSTIndexScanExec extends PhysicalExec{
  private ScanNode scanNode;
  private SeekableScanner fileScanner;
  
  private EvalNode qual;
  private EvalContext qualCtx;
  private Schema inputSchema;
  private Schema outputSchema;
  private BSTIndex.BSTIndexReader reader;
  
  private final Projector projector;
  private EvalContext [] evalContexts;
  
  private Datum[] datum = null;
  
  private boolean initialize = true;
  
  public BSTIndexScanExec(StorageManager sm , ScanNode scanNode ,
       Fragment fragment, Path fileName , Schema keySchema,
       TupleComparator comparator , Datum[] datum) throws IOException {
    this.scanNode = scanNode;
    this.qual = scanNode.getQual();
    if(this.qual == null) {
      this.qualCtx = null;
    } else {
      this.qualCtx = this.qual.newContext();
    }
    this.inputSchema = scanNode.getInputSchema();
    this.outputSchema = scanNode.getOutputSchema();
    this.datum = datum;
    
    Fragment[] frags = new Fragment[1];
    frags[0] = fragment;
    this.fileScanner = (SeekableScanner)sm.getScanner(fragment.getMeta(), 
        frags, this.inputSchema);
    this.projector = new Projector(inputSchema, outputSchema, scanNode.getTargets());
    this.evalContexts = projector.renew();
    
    this.reader = new BSTIndex(sm.getFileSystem().getConf()).
        getIndexReader(fileName, keySchema, comparator);
    this.reader.open();
   
  }

  @Override
  public Schema getSchema() {
    return this.outputSchema;
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
    Tuple outTuple = new VTuple(this.outputSchema.getColumnNum());
    if (!scanNode.hasQual()) {
      if ((tuple = fileScanner.next()) != null) {
        projector.eval(evalContexts, tuple);
        projector.terminate(evalContexts, outTuple);
        return outTuple;
      } else {
        reader.close();
        fileScanner.close();
        return null;
      }
    } else {
       while( reader.isCurInMemory() && (tuple = fileScanner.next()) != null) {
         qual.eval(qualCtx, inputSchema, tuple);
         if (qual.terminate(qualCtx).asBool()) {
           projector.eval(evalContexts, tuple);
           projector.terminate(evalContexts, outTuple);
           return outTuple;
         } else {
           fileScanner.seek(reader.next());
         }
       }
     }
    reader.close();
    fileScanner.close();
    return null;
  }
  @Override
  public void rescan() throws IOException {
    fileScanner.reset();
  }
  
}
