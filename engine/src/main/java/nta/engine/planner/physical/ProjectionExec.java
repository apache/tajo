/**
 * 
 */
package nta.engine.planner.physical;

import java.io.IOException;

import nta.catalog.Schema;
import nta.engine.SubqueryContext;
import nta.engine.exec.eval.EvalContext;
import nta.engine.planner.Projector;
import nta.engine.planner.logical.ProjectionNode;
import nta.storage.Tuple;
import nta.storage.VTuple;

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
  private final EvalContext [] evalContexts;
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
