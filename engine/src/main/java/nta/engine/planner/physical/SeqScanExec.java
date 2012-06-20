/**
 * 
 */
package nta.engine.planner.physical;

import java.io.IOException;

import nta.catalog.Schema;
import nta.engine.exec.eval.EvalContext;
import nta.engine.exec.eval.EvalNode;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.planner.Projector;
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
      Fragment [] fragments) throws IOException {
    this.scanNode = scanNode;
    this.qual = scanNode.getQual();
    if (qual == null) {
      qualCtx = null;
    } else {
      qualCtx = this.qual.newContext();
    }
    this.inSchema = scanNode.getInputSchema();
    this.outSchema = scanNode.getOutputSchema();
    this.scanner = sm.getScanner(fragments[0].getMeta(), fragments);
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
