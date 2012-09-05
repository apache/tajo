/**
 * 
 */
package tajo.engine.planner.physical;

import tajo.SubqueryContext;
import tajo.engine.exec.eval.EvalContext;
import tajo.engine.parser.QueryBlock.Target;
import tajo.engine.planner.logical.EvalExprNode;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.io.IOException;

/**
 * @author Hyunsik Choi
 */
public class EvalExprExec extends PhysicalExec {
  private final EvalExprNode plan;
  private final EvalContext[] evalContexts;
  
  /**
   * 
   */
  public EvalExprExec(final SubqueryContext context, final EvalExprNode plan) {
    super(context, plan.getInSchema(), plan.getOutSchema());
    this.plan = plan;

    evalContexts = new EvalContext[plan.getExprs().length];
    for (int i = 0; i < plan.getExprs().length; i++) {
      evalContexts[i] = plan.getExprs()[i].getEvalTree().newContext();
    }
  }

  @Override
  public void init() throws IOException {
  }

  /* (non-Javadoc)
  * @see PhysicalExec#next()
  */
  @Override
  public Tuple next() throws IOException {    
    Target [] targets = plan.getExprs();
    Tuple t = new VTuple(targets.length);
    for (int i = 0; i < targets.length; i++) {
      targets[i].getEvalTree().eval(evalContexts[i], inSchema, null);
      t.put(i, targets[i].getEvalTree().terminate(evalContexts[i]));
    }
    return t;
  }

  @Override
  public void rescan() throws IOException {    
  }

  @Override
  public void close() throws IOException {
  }
}
