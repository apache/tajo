/**
 * 
 */
package nta.engine.planner;

import java.io.IOException;

import nta.engine.SubqueryContext;
import nta.engine.exception.InternalException;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.planner.logical.GroupbyNode;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.LogicalRootNode;
import nta.engine.planner.logical.ProjectionNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.planner.logical.SelectionNode;
import nta.engine.planner.logical.SortNode;
import nta.engine.planner.logical.StoreTableNode;
import nta.engine.planner.physical.GroupByExec;
import nta.engine.planner.physical.PartitionedStoreExec;
import nta.engine.planner.physical.PhysicalExec;
import nta.engine.planner.physical.SeqScanExec;
import nta.engine.planner.physical.SortExec;
import nta.engine.planner.physical.StoreTableExec;
import nta.storage.StorageManager;

/**
 * This class generates a physical execution plan.
 * 
 * @author Hyunsik Choi
 * 
 */
public class PhysicalPlanner {
  private final StorageManager sm;

  public PhysicalPlanner(StorageManager sm) {
    this.sm = sm;
  }

  public PhysicalExec createPlan(SubqueryContext ctx, LogicalNode logicalPlan)
      throws InternalException {
    PhysicalExec plan = null;
    try {
      plan = createPlanRecursive(ctx, logicalPlan);
    } catch (IOException ioe) {
      throw new InternalException(ioe.getMessage());
    }

    return plan;
  }

  private PhysicalExec createPlanRecursive(SubqueryContext ctx,
      LogicalNode logicalNode) throws IOException {
    PhysicalExec outer = null;
    PhysicalExec inner = null;

    switch (logicalNode.getType()) {
    case ROOT:
      LogicalRootNode rootNode = (LogicalRootNode) logicalNode;
      return createPlanRecursive(ctx, rootNode.getSubNode());
    
    case STORE:
      StoreTableNode storeNode = (StoreTableNode) logicalNode;
      inner = createPlanRecursive(ctx, storeNode.getSubNode());
      return createStorePlan(ctx, storeNode, inner);
      
    case SELECTION:
      SelectionNode selNode = (SelectionNode) logicalNode;
      return createPlanRecursive(ctx, selNode.getSubNode());

    case PROJECTION:
      ProjectionNode prjNode = (ProjectionNode) logicalNode;
      return createPlanRecursive(ctx, prjNode.getSubNode());

    case SCAN:
      inner = createScanPlan(ctx, (ScanNode) logicalNode);
      return inner;

    case GROUP_BY:
      GroupbyNode grpNode = (GroupbyNode) logicalNode;
      inner = createPlanRecursive(ctx, grpNode.getSubNode());
      return createGroupByPlan(ctx, grpNode, inner);
      
    case SORT:
      SortNode sortNode = (SortNode) logicalNode;
      inner = createPlanRecursive(ctx, sortNode.getSubNode());
      return createSortPlan(ctx, sortNode, inner);          
    
    case JOIN:   
    case RENAME:
    case SET_UNION:
    case SET_DIFF:
    case SET_INTERSECT:
    case CREATE_TABLE:
    case INSERT_INTO:
    case SHOW_TABLE:
    case DESC_TABLE:
    case SHOW_FUNCTION:
    default:
      return null;
    }
  }
  
  public PhysicalExec createStorePlan(SubqueryContext ctx, StoreTableNode annotation,
      PhysicalExec subOp) throws IOException {
    PhysicalExec store = null;
    if (annotation.hasPartitionKey()) {
      store = new PartitionedStoreExec(sm, ctx.getQueryId(), annotation, subOp);
    } else {
      store = new StoreTableExec(sm, ctx.getQueryId(), annotation, subOp);
    }
    return store;
  }

  public PhysicalExec createScanPlan(SubqueryContext ctx, ScanNode scanNode)
      throws IOException {
    Fragment [] fragments = ctx.getTables(scanNode.getTableId());
    SeqScanExec scan = new SeqScanExec(sm, scanNode, fragments);

    return scan;
  }
  
  public PhysicalExec createGroupByPlan(SubqueryContext ctx, 
      GroupbyNode groupbyNode, PhysicalExec subOp) throws IOException {
    GroupByExec groupby = new GroupByExec(groupbyNode, subOp);
    
    return groupby;
  }
  
  public PhysicalExec createSortPlan(SubqueryContext ctx,
      SortNode sortNode, PhysicalExec subOp) throws IOException {
    SortExec sort = new SortExec(sortNode, subOp);
    
    return sort;
  }
}
