package tajo.engine.planner.physical;

import org.apache.hadoop.fs.Path;
import tajo.catalog.Schema;
import tajo.datum.Datum;
import tajo.engine.exec.eval.EvalContext;
import tajo.engine.exec.eval.EvalNode;
import tajo.engine.ipc.protocolrecords.Fragment;
import tajo.engine.planner.Projector;
import tajo.engine.planner.logical.ScanNode;
import tajo.index.bst.BSTIndex;
import tajo.storage.SeekableScanner;
import tajo.storage.StorageManager;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.io.IOException;

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
