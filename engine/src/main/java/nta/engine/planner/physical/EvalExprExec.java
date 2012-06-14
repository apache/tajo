/**
 * 
 */
package nta.engine.planner.physical;

import java.io.IOException;

import nta.catalog.Schema;
import nta.engine.exec.eval.EvalContext;
import nta.engine.parser.QueryBlock.Target;
import nta.engine.planner.logical.EvalExprNode;
import nta.storage.Tuple;
import nta.storage.VTuple;

/**
 * @author Hyunsik Choi
 */
public class EvalExprExec extends PhysicalExec {
  private final EvalExprNode annotation;
  private final Schema inputSchema;
  private final Schema outputSchema;
  private final EvalContext [] evalContexts;
  
  /**
   * 
   */
  public EvalExprExec(EvalExprNode annotation) {
    this.annotation = annotation;
    this.inputSchema = annotation.getInputSchema();
    this.outputSchema = annotation.getOutputSchema();
    evalContexts = new EvalContext[this.annotation.getExprs().length];
    for (int i = 0; i < this.annotation.getExprs().length; i++) {
      evalContexts[i] = this.annotation.getExprs()[i].getEvalTree().newContext();
    }
  }

  /* (non-Javadoc)
   * @see nta.engine.SchemaObject#getSchema()
   */
  @Override
  public Schema getSchema() {    
    return outputSchema;
  }

  /* (non-Javadoc)
  * @see nta.engine.planner.physical.PhysicalExec#next()
  */
  @Override
  public Tuple next() throws IOException {    
    Target [] targets = annotation.getExprs();
    Tuple t = new VTuple(targets.length);
    for (int i = 0; i < targets.length; i++) {
      targets[i].getEvalTree().eval(evalContexts[i], inputSchema, null);
      t.put(i, targets[i].getEvalTree().terminate(evalContexts[i]));
    }
    return t;
  }

  @Override
  public void rescan() throws IOException {    
  }
}
