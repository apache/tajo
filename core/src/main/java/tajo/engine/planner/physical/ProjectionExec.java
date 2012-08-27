/**
 * 
 */
package tajo.engine.planner.physical;

import tajo.catalog.Schema;
import tajo.engine.SubqueryContext;
import tajo.engine.exec.eval.EvalContext;
import tajo.engine.planner.Projector;
import tajo.engine.planner.logical.ProjectionNode;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.io.IOException;

/**
 * @author Hyunsik Choi
 */
public class ProjectionExec extends PhysicalExec {
  private final ProjectionNode projNode;
  private PhysicalExec child;
      
  private final Schema inSchema;
  private final Schema outSchema;
  
  private final Tuple outTuple;

  // for projection
  private final EvalContext[] evalContexts;
  private final Projector projector;
  
  public ProjectionExec(SubqueryContext ctx, ProjectionNode projNode,
      PhysicalExec child) {
    this.projNode = projNode;    
    this.inSchema = projNode.getInputSchema();
    this.outSchema = projNode.getOutputSchema();
    this.child = child;
    this.outTuple = new VTuple(outSchema.getColumnNum());
    this.projector = new Projector(inSchema, outSchema, projNode.getTargets());
    this.evalContexts = projector.renew();
  }

  public PhysicalExec getChild(){
    return this.child;
  }
  public void setChild(PhysicalExec s){
    this.child = s;
  }

  @Override
  public Schema getSchema() {
    return projNode.getOutputSchema();
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

  @Override
  public void rescan() throws IOException {
    child.rescan();
  }
}
