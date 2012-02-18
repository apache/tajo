/**
 * 
 */
package nta.engine.planner.physical;

import java.io.IOException;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.engine.exec.eval.EvalNode;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.planner.logical.ScanNode;
import nta.storage.Scanner;
import nta.storage.StorageManager;
import nta.storage.Tuple;
import nta.storage.VTuple;

/**
 * @author Hyunsik Choi
 * 
 */
public class SeqScanExec extends PhysicalExec {
  private final ScanNode annotation;
  private Scanner scanner = null;
  
  private Tuple tuple = null;
  private int [] targetIds = null;
  private EvalNode qual = null;
  private Schema inputSchema;
  private Schema outputSchema;

  /**
   * @throws IOException 
	 * 
	 */
  public SeqScanExec(StorageManager sm, ScanNode annotation,
      Fragment [] fragments) throws IOException {
    this.annotation = annotation;
    this.qual = annotation.getQual();
    this.inputSchema = annotation.getInputSchema();
    this.outputSchema = annotation.getOutputSchema();
    
    this.scanner = sm.getScanner(fragments[0].getMeta(), fragments);    
    this.targetIds = new int[annotation.getTargetList().getColumnNum()];
        
    int i=0;
    for (Column target : annotation.getTargetList().getColumns()) {
      targetIds[i] = inputSchema.getColumnId(target.getQualifiedName());
      i++;
    }
    
    this.tuple = new VTuple(outputSchema.getColumnNum());
  }

  @Override
  public Tuple next() throws IOException {
    if (!annotation.hasQual()) {
      if ((tuple = scanner.next()) != null) {
        Tuple newTuple = new VTuple(outputSchema.getColumnNum());
        int i=0;
        for(int cid : targetIds) {
          newTuple.put(i, tuple.get(cid));
          i++;
        }
        return newTuple;
      } else {
        return null;
      }
    } else {
      while ((tuple = scanner.next()) != null) {        
        if (qual.eval(inputSchema, tuple).asBool()) {
          Tuple newTuple = new VTuple(outputSchema.getColumnNum());
          int i=0;
          for(int cid : targetIds) {
            newTuple.put(i, tuple.get(cid));
            i++;
          }
          return newTuple;
        }
      }
      return null;
    }
  }

  @Override
  public Schema getSchema() {
    return outputSchema;
  }
}
