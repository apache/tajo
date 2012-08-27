/**
 * 
 */
package tajo.engine.planner.physical;

import tajo.catalog.Schema;
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
  private final EvalExprNode annotation;
  private final Schema inputSchema;
  private final Schema outputSchema;
  private final EvalContext[] evalContexts;
  
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
   * @see SchemaObject#getSchema()
   */
  @Override
  public Schema getSchema() {    
    return outputSchema;
  }

  /* (non-Javadoc)
  * @see PhysicalExec#next()
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
