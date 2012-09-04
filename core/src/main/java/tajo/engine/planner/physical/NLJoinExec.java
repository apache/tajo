package tajo.engine.planner.physical;

import tajo.SubqueryContext;
import tajo.catalog.Schema;
import tajo.engine.exec.eval.EvalContext;
import tajo.engine.exec.eval.EvalNode;
import tajo.engine.planner.Projector;
import tajo.engine.planner.logical.JoinNode;
import tajo.storage.FrameTuple;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

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

  // temporal tuples and states for nested loop join
  private boolean needNewOuter;
  private FrameTuple frameTuple;
  private Tuple outerTuple = null;
  private Tuple innerTuple = null;
  private Tuple outTuple = null;
  private EvalContext qualCtx;

  // projection
  private final EvalContext [] evalContexts;
  private final Projector projector;

  public NLJoinExec(SubqueryContext ctx, JoinNode joinNode, PhysicalExec outer,
      PhysicalExec inner) {    
    this.outer = outer;
    this.inner = inner;
    this.inSchema = joinNode.getInSchema();
    this.outSchema = joinNode.getOutSchema();
    if (joinNode.hasJoinQual()) {
      this.joinQual = joinNode.getJoinQual();
      this.qualCtx = this.joinQual.newContext();
    }
    this.ann = joinNode;

    // for projection
    projector = new Projector(inSchema, outSchema, joinNode.getTargets());
    evalContexts = projector.renew();

    // for join
    needNewOuter = true;
    frameTuple = new FrameTuple();
    outTuple = new VTuple(outSchema.getColumnNum());
  }

  public PhysicalExec getOuter(){
    return this.outer;
  }

  public PhysicalExec getInner(){
    return this.inner;
  }

  public JoinNode getJoinNode(){
    return this.ann;
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
          projector.eval(evalContexts, frameTuple);
          projector.terminate(evalContexts, outTuple);
          return outTuple;
        }
      } else {
        projector.eval(evalContexts, frameTuple);
        projector.terminate(evalContexts, outTuple);
        return outTuple;
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
