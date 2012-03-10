package nta.engine.planner.physical;

import java.io.IOException;

import nta.catalog.Schema;
import nta.engine.SubqueryContext;
import nta.engine.exec.eval.EvalNode;
import nta.engine.planner.logical.JoinNode;
import nta.storage.FrameTuple;
import nta.storage.Tuple;

public class NLJoinExec extends PhysicalExec {
  // from logical plan
  private Schema inSchema;
  private Schema outSchema;
  private EvalNode joinQual;
  
  // sub operations
  private PhysicalExec outer;
  private PhysicalExec inner;

  // temporal tuples and states for nested loop join
  private boolean needNewOuter;
  private FrameTuple frameTuple;
  private Tuple outerTuple = null;
  private Tuple innerTuple = null;

  public NLJoinExec(SubqueryContext ctx, JoinNode ann, PhysicalExec outer,
      PhysicalExec inner) {    
    this.outer = outer;
    this.inner = inner;
    this.inSchema = ann.getInputSchema();
    this.outSchema = ann.getOutputSchema();
    this.joinQual = ann.getJoinQual();

    needNewOuter = true;
    
    frameTuple = new FrameTuple();
  }

  public Tuple next() throws IOException {
    for (;;) {
      if (needNewOuter) {
        outerTuple = outer.next();
        if (outerTuple == null) {
          return null;
        }
        needNewOuter = false;
      }

      innerTuple = inner.next();
      if (innerTuple == null) {
        needNewOuter = true;
        inner.rescan();
        continue;
      }

      frameTuple.set(outerTuple, innerTuple);
      if (joinQual != null) {
        if (joinQual.eval(inSchema, frameTuple).asBool())
          return frameTuple;
      } else {
        return frameTuple;
      }
    }
  }

  @Override
  public Schema getSchema() {
    return outSchema;
  }

  @Override
  public void rescan() throws IOException {
    outer.rescan();
    inner.rescan();
    needNewOuter = true;
  }
}
