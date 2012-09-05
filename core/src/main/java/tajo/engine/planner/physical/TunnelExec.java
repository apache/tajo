package tajo.engine.planner.physical;

import tajo.SubqueryContext;
import tajo.catalog.Schema;
import tajo.storage.Tuple;

import java.io.IOException;

public class TunnelExec extends UnaryPhysicalExec{

  public TunnelExec (final SubqueryContext context,
                     final Schema outputSchema,
                     final PhysicalExec child) {
    super(context, outputSchema, outputSchema, child);
  }

  @Override
  public Tuple next() throws IOException {
    return child.next();
  }
  @Override
  public void rescan() throws IOException {   
  }
}
