package tajo.engine.planner.physical;

import tajo.catalog.Schema;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.io.IOException;

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
