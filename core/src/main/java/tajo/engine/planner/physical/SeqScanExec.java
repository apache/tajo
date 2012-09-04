/**
 * 
 */
package tajo.engine.planner.physical;

import tajo.catalog.Schema;
import tajo.engine.exec.eval.EvalContext;
import tajo.engine.exec.eval.EvalNode;
import tajo.engine.ipc.protocolrecords.Fragment;
import tajo.engine.planner.Projector;
import tajo.engine.planner.logical.ScanNode;
import tajo.storage.Scanner;
import tajo.storage.StorageManager;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.io.IOException;

/**
 * @author Hyunsik Choi
 * 
 */
public class SeqScanExec extends PhysicalExec {
  private final ScanNode scanNode;
  private Scanner scanner = null;

  private EvalNode qual = null;
  private EvalContext qualCtx;
  private Schema inSchema;
  private Schema outSchema;

  private final Projector projector;
  private EvalContext [] evalContexts;

  /**
   * @throws IOException 
	 * 
	 */
  public SeqScanExec(StorageManager sm, ScanNode scanNode,
      Fragment[] fragments) throws IOException {
    this.scanNode = scanNode;
    this.qual = scanNode.getQual();
    if (qual == null) {
      qualCtx = null;
    } else {
      qualCtx = this.qual.newContext();
    }
    this.inSchema = scanNode.getInSchema();
    this.outSchema = scanNode.getOutSchema();
    this.scanner = sm.getScanner(fragments[0].getMeta(), fragments, scanNode.getInSchema());
    this.projector = new Projector(inSchema, outSchema, scanNode.getTargets());
    this.evalContexts = projector.renew();
  }

  @Override
  public Tuple next() throws IOException {
    Tuple tuple;
    Tuple outTuple = new VTuple(outSchema.getColumnNum());

    if (!scanNode.hasQual()) {
      if ((tuple = scanner.next()) != null) {
        projector.eval(evalContexts, tuple);
        projector.terminate(evalContexts, outTuple);
        outTuple.setOffset(tuple.getOffset());
        return outTuple;
      } else {
        return null;
      }
    } else {
      while ((tuple = scanner.next()) != null) {
        qual.eval(qualCtx, inSchema, tuple);
        if (qual.terminate(qualCtx).asBool()) {
          projector.eval(evalContexts, tuple);
          projector.terminate(evalContexts, outTuple);
          return outTuple;
        }
      }
      return null;
    }
  }

  @Override
  public Schema getSchema() {
    return outSchema;
  }

  @Override
  public void rescan() throws IOException {
    scanner.reset();    
  }
}
