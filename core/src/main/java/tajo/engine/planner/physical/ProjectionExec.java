/**
 * 
 */
package tajo.engine.planner.physical;

import tajo.SubqueryContext;
import tajo.engine.exec.eval.EvalContext;
import tajo.engine.planner.Projector;
import tajo.engine.planner.logical.ProjectionNode;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.io.IOException;

/**
 * @author Hyunsik Choi
 */
public class ProjectionExec extends UnaryPhysicalExec {
  private final ProjectionNode plan;

  // for projection
  private Tuple outTuple;
  private EvalContext[] evalContexts;
  private Projector projector;
  
  public ProjectionExec(SubqueryContext context, ProjectionNode plan,
      PhysicalExec child) {
    super(context, plan.getInSchema(), plan.getOutSchema(), child);
    this.plan = plan;
  }

  public void init() throws IOException {
    super.init();

    this.outTuple = new VTuple(outSchema.getColumnNum());
    this.projector = new Projector(inSchema, outSchema, this.plan.getTargets());
    this.evalContexts = projector.renew();
  }

  @Override
  public Tuple next() throws IOException {
    Tuple tuple = child.next();
    if (tuple ==  null) {
      return null;
    }

    projector.eval(evalContexts, tuple);
    projector.terminate(evalContexts, outTuple);
    return outTuple;
  }
}
