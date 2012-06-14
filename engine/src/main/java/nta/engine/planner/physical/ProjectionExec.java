/**
 * 
 */
package nta.engine.planner.physical;

import java.io.IOException;

import nta.catalog.Schema;
import nta.engine.SubqueryContext;
import nta.engine.exec.eval.EvalContext;
import nta.engine.exec.eval.EvalNode;
import nta.engine.planner.logical.ProjectionNode;
import nta.storage.Tuple;
import nta.storage.VTuple;

/**
 * @author Hyunsik Choi
 */
public class ProjectionExec extends PhysicalExec {
  private final ProjectionNode projNode;
  private PhysicalExec subOp;
      
  private final Schema inSchema;
  private final Schema outSchema;
  
  private final Tuple outTuple;
  private final EvalNode [] evals;
  private final EvalContext [] evalContexts;
  
  public ProjectionExec(SubqueryContext ctx, ProjectionNode projNode,
      PhysicalExec subOp) {
    this.projNode = projNode;    
    this.inSchema = projNode.getInputSchema();
    this.outSchema = projNode.getOutputSchema();
    this.subOp = subOp;
    
    this.outTuple = new VTuple(outSchema.getColumnNum());
    
    evals = new EvalNode[projNode.getTargetList().length];
    evalContexts = new EvalContext[projNode.getTargetList().length];

    for (int i = 0; i < projNode.getTargetList().length; i++) {
      evals[i] = projNode.getTargetList()[i].getEvalTree();
      evalContexts[i] = evals[i].newContext();
    }
  }

  public PhysicalExec getSubOp(){
    return this.subOp;
  }
  public void setSubOp(PhysicalExec s){
    this.subOp = s;
  }

  @Override
  public Schema getSchema() {
    return projNode.getOutputSchema();
  }

  @Override
  public Tuple next() throws IOException {
    Tuple tuple = subOp.next();
    if (tuple ==  null) {
      return null;
    }
     
    for (int i = 0; i < evals.length; i++) {
      evals[i].eval(evalContexts[i], inSchema, tuple);
      outTuple.put(i, evals[i].terminate(evalContexts[i]));
    }
    
    return outTuple;
  }

  @Override
  public void rescan() throws IOException {
    subOp.rescan();
  }
}
