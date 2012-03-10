/**
 * 
 */
package nta.engine.planner.physical;

import java.io.IOException;

import nta.catalog.Schema;
import nta.engine.SubqueryContext;
import nta.engine.planner.logical.ProjectionNode;
import nta.engine.utils.TupleUtil;
import nta.storage.Tuple;
import nta.storage.VTuple;

/**
 * @author Hyunsik Choi
 */
public class ProjectionExec extends PhysicalExec {
  private final ProjectionNode projNode;
  private final PhysicalExec subOp;
  
  private final Tuple outTuple;
  private final int [] targetIds;
  private final Schema inSchema;
  private final Schema outSchema;

  public ProjectionExec(SubqueryContext ctx, ProjectionNode projNode, 
      PhysicalExec subOp) {
    this.projNode = projNode;    
    this.inSchema = projNode.getInputSchema();
    this.outSchema = projNode.getOutputSchema();
    this.subOp = subOp;
    
    this.outTuple = new VTuple(outSchema.getColumnNum());
    this.targetIds = TupleUtil.getTargetIds(inSchema, outSchema);
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
        
    return TupleUtil.project(tuple, outTuple, targetIds);
  }

  @Override
  public void rescan() throws IOException {
    subOp.rescan();
  }
}
