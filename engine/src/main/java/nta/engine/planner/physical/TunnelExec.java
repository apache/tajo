package nta.engine.planner.physical;

import java.io.IOException;

import nta.catalog.Schema;
import nta.storage.Tuple;
import nta.storage.VTuple;

public class TunnelExec  extends PhysicalExec{
  private Schema outputSchema;
  private PhysicalExec supOp;
  public TunnelExec ( Schema outputSchema , PhysicalExec supOp) {
    this.outputSchema = outputSchema;
    this.supOp = supOp;
  }

  @Override
  public Schema getSchema() {
    return this.outputSchema;
  }
  @Override
  public Tuple next() throws IOException {
    VTuple tuple = (VTuple)supOp.next();
    return tuple;
  }
  @Override
  public void rescan() throws IOException {   
  }
}
