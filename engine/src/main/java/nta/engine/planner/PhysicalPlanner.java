/**
 * 
 */
package nta.engine.planner;

import java.io.IOException;

import nta.engine.SubqueryContext;
import nta.engine.exception.InternalException;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.planner.logical.IndexWriteNode;
import nta.engine.planner.logical.StoreTableNode;
import nta.engine.planner.logical.EvalExprNode;
import nta.engine.planner.logical.GroupbyNode;
import nta.engine.planner.logical.JoinNode;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.LogicalRootNode;
import nta.engine.planner.logical.ProjectionNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.planner.logical.SelectionNode;
import nta.engine.planner.logical.SortNode;
import nta.engine.planner.logical.UnionNode;
import nta.engine.planner.physical.EvalExprExec;
import nta.engine.planner.physical.GroupByExec;
import nta.engine.planner.physical.IndexWriteExec;
import nta.engine.planner.physical.NLJoinExec;
import nta.engine.planner.physical.PartitionedStoreExec;
import nta.engine.planner.physical.PhysicalExec;
import nta.engine.planner.physical.ProjectionExec;
import nta.engine.planner.physical.SeqScanExec;
import nta.engine.planner.physical.SortExec;
import nta.engine.planner.physical.StoreTableExec;
import nta.engine.planner.physical.UnionExec;
import nta.storage.StorageManager;

import com.google.common.base.Preconditions;

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
      throw new InternalException(ioe);
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
      
    case EXPRS:
      EvalExprNode evalExpr = (EvalExprNode) logicalNode;
      return new EvalExprExec(evalExpr);
    
    case STORE:
      StoreTableNode storeNode = (StoreTableNode) logicalNode;
      outer = createPlanRecursive(ctx, storeNode.getSubNode());
      return createStorePlan(ctx, storeNode, outer);
      
    case SELECTION:
      SelectionNode selNode = (SelectionNode) logicalNode;
      return createPlanRecursive(ctx, selNode.getSubNode());

    case PROJECTION:
      ProjectionNode prjNode = (ProjectionNode) logicalNode;
      outer = createPlanRecursive(ctx, prjNode.getSubNode());      
      return new ProjectionExec(ctx, prjNode, outer);
      
    case SCAN:
      outer = createScanPlan(ctx, (ScanNode) logicalNode);
      return outer;

    case GROUP_BY:
      GroupbyNode grpNode = (GroupbyNode) logicalNode;
      outer = createPlanRecursive(ctx, grpNode.getSubNode());
      return createGroupByPlan(ctx, grpNode, outer);
      
    case SORT:
      SortNode sortNode = (SortNode) logicalNode;
      outer = createPlanRecursive(ctx, sortNode.getSubNode());
      return createSortPlan(ctx, sortNode, outer);          
    
    case JOIN:
      JoinNode joinNode = (JoinNode) logicalNode;
      outer = createPlanRecursive(ctx, joinNode.getOuterNode());
      inner = createPlanRecursive(ctx, joinNode.getInnerNode());
      return createJoinPlan(ctx, joinNode, outer, inner);
      
    case UNION:
      UnionNode unionNode = (UnionNode) logicalNode;
      outer = createPlanRecursive(ctx, unionNode.getOuterNode());
      inner = createPlanRecursive(ctx, unionNode.getInnerNode());
      return new UnionExec(outer, inner);
      
    case CREATE_INDEX:
      IndexWriteNode createIndexNode = (IndexWriteNode) logicalNode;
      outer = createPlanRecursive(ctx, createIndexNode.getSubNode());
      return createIndexWritePlan(sm, ctx, createIndexNode, outer);
      
    case RENAME:
    case SET_UNION:
    case SET_DIFF:
    case SET_INTERSECT:
    case INSERT_INTO:
    case SHOW_TABLE:
    case DESC_TABLE:
    case SHOW_FUNCTION:    
    default:
      return null;
    }
  }
  
  public PhysicalExec createJoinPlan(SubqueryContext ctx, JoinNode joinNode, 
      PhysicalExec outer, PhysicalExec inner) {
    NLJoinExec nlj = new NLJoinExec(ctx, joinNode, outer, inner);
    return nlj;
  }
  
  public PhysicalExec createStorePlan(SubqueryContext ctx, StoreTableNode annotation,
      PhysicalExec subOp) throws IOException {
    PhysicalExec store = null;
    if (annotation.hasPartitionKey()) { // if the partition keys are specified
      store = new PartitionedStoreExec(ctx, sm, annotation, subOp);
    } else {
      store = new StoreTableExec(ctx, sm, annotation, subOp);
    }
    return store;
  }

  public PhysicalExec createScanPlan(SubqueryContext ctx, ScanNode scanNode)
      throws IOException {
    Preconditions.checkNotNull(ctx.getTable(scanNode.getTableId()), 
        "Error: There is no table matched to %s", scanNode.getTableId());
    
    Fragment [] fragments = ctx.getTables(scanNode.getTableId());
    SeqScanExec scan = new SeqScanExec(sm, scanNode, fragments);

    return scan;
  }
  
  public PhysicalExec createGroupByPlan(SubqueryContext ctx, 
      GroupbyNode groupbyNode, PhysicalExec subOp) throws IOException {
    GroupByExec groupby = new GroupByExec(ctx, groupbyNode, subOp);
    
    return groupby;
  }
  
  public PhysicalExec createSortPlan(SubqueryContext ctx,
      SortNode sortNode, PhysicalExec subOp) throws IOException {
    SortExec sort = new SortExec(sortNode, subOp);
    
    return sort;
  }
  
  public PhysicalExec createIndexWritePlan(
      StorageManager sm,
      SubqueryContext ctx,
      IndexWriteNode indexWriteNode, PhysicalExec subOp) throws IOException {
      
    IndexWriteExec writer = new IndexWriteExec(indexWriteNode, 
        ctx.getTable(indexWriteNode.getTableName()), subOp);
    
    return writer;
  }
}
