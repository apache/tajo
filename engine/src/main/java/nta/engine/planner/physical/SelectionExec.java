package nta.engine.planner.physical;

import nta.catalog.Schema;
import nta.engine.SubqueryContext;
import nta.engine.exec.eval.EvalNode;
import nta.engine.planner.logical.SelectionNode;
import nta.engine.utils.TupleUtil;
import nta.storage.Tuple;
import nta.storage.VTuple;

import java.io.IOException;

/**
<<<<<<< HEAD
 * @author : hyunsik
=======
 * @author : Hyunsik Choi
>>>>>>> b3768f2c52e09ec4a3eadc9d1ee187c435e25ce4
 */
public class SelectionExec extends PhysicalExec  {
  private final SelectionNode annotation;
  private final PhysicalExec subOp;
  private final Schema inSchema;
  private final Schema outSchema;

  private final EvalNode qual;
  private final Tuple outputTuple;
  // projection
  private int [] targetIds;

  public SelectionExec(SubqueryContext ctx, SelectionNode selNode,
                       PhysicalExec subOp) {
    this.annotation = selNode;
    this.inSchema = selNode.getInputSchema();
    this.outSchema = selNode.getOutputSchema();
    this.subOp = subOp;

    this.qual = this.annotation.getQual();
    // for projection
    if (!inSchema.equals(outSchema)) {
      targetIds = TupleUtil.getTargetIds(inSchema, outSchema);
    }

    this.outputTuple = new VTuple(outSchema.getColumnNum());
  }

  @Override
  public Schema getSchema() {
    return annotation.getOutputSchema();
  }

  @Override
  public Tuple next() throws IOException {
    Tuple tuple = null;
    while ((tuple = subOp.next()) != null) {
      if (qual.eval(inSchema, tuple).asBool()) {
        if (targetIds != null) {
          TupleUtil.project(tuple, outputTuple, targetIds);
          return outputTuple;
        } else {
          return tuple;
        }
      }
    }

    return null;
  }

  @Override
  public void rescan() throws IOException {
    subOp.rescan();
  }
}
