/**
 * 
 */
package nta.engine.planner.physical;

import java.io.IOException;

import nta.catalog.Schema;
import nta.engine.SubqueryContext;
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
  
  public PhysicalExec getSubOp(){
    return this.subOp;
  }
  public void setsubOp(PhysicalExec s){
    this.subOp = s;
  }

  public ProjectionExec(SubqueryContext ctx, ProjectionNode projNode, 
      PhysicalExec subOp) {
    this.projNode = projNode;    
    this.inSchema = projNode.getInputSchema();
    this.outSchema = projNode.getOutputSchema();
    this.subOp = subOp;
    
    this.outTuple = new VTuple(outSchema.getColumnNum());
    
    evals = new EvalNode[projNode.getTargetList().length];
    
    for (int i = 0; i < projNode.getTargetList().length; i++) {
      evals[i] = projNode.getTargetList()[i].getEvalTree();
    }
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
      outTuple.put(i, evals[i].eval(inSchema, tuple));
    }
    
    return outTuple;
  }

  @Override
  public void rescan() throws IOException {
    subOp.rescan();
  }
}
