package nta.engine.planner.physical;

import nta.catalog.Schema;
import nta.engine.SubqueryContext;
import nta.engine.exec.eval.EvalContext;
import nta.engine.exec.eval.EvalNode;
import nta.engine.planner.logical.JoinNode;
import nta.engine.utils.TupleUtil;
import nta.storage.FrameTuple;
import nta.storage.Tuple;
import nta.storage.VTuple;

import java.io.IOException;

public class NLJoinExec extends PhysicalExec {
  // from logical plan
  private Schema inSchema;
  private Schema outSchema;
  private EvalNode joinQual;
  
  // sub operations
  private PhysicalExec outer;
  private PhysicalExec inner;
    
  private JoinNode ann;
  
  public PhysicalExec getOuter(){
    return this.outer;
  }
  public PhysicalExec getInner(){
    return this.inner;
  }
  public JoinNode getJoinNode(){
    return this.ann;
  }

  // temporal tuples and states for nested loop join
  private boolean needNewOuter;
  private FrameTuple frameTuple;
  private Tuple outerTuple = null;
  private Tuple innerTuple = null;
  private Tuple outputTuple = null;
  private EvalContext qualCtx;

  // projection
  private final int [] targetIds;

  public NLJoinExec(SubqueryContext ctx, JoinNode ann, PhysicalExec outer,
      PhysicalExec inner) {    
    this.outer = outer;
    this.inner = inner;
    this.inSchema = ann.getInputSchema();
    this.outSchema = ann.getOutputSchema();
    if (ann.hasJoinQual()) {
      this.joinQual = ann.getJoinQual();
      this.qualCtx = this.joinQual.newContext();
    }
    this.ann = ann;

    // for projection
    targetIds = TupleUtil.getTargetIds(inSchema, outSchema);

    // for join
    needNewOuter = true;
    frameTuple = new FrameTuple();
    outputTuple = new VTuple(outSchema.getColumnNum());
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
        joinQual.eval(qualCtx, inSchema, frameTuple);
        if (joinQual.terminate(qualCtx).asBool()) {
          TupleUtil.project(frameTuple, outputTuple, targetIds);
          return outputTuple;
        }
      } else {
        TupleUtil.project(frameTuple, outputTuple, targetIds);
        return outputTuple;
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
