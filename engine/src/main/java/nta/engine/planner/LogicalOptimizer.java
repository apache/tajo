/**
 * 
 */
package nta.engine.planner;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.SchemaUtil;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.engine.Context;
import nta.engine.exec.eval.EvalNode;
import nta.engine.exec.eval.EvalTreeUtil;
import nta.engine.exec.eval.FieldEval;
import nta.engine.exec.eval.FuncCallEval;
import nta.engine.parser.QueryBlock.SortSpec;
import nta.engine.parser.QueryBlock.Target;
import nta.engine.planner.logical.BinaryNode;
import nta.engine.planner.logical.StoreTableNode;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.GroupbyNode;
import nta.engine.planner.logical.JoinNode;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.LogicalNodeVisitor;
import nta.engine.planner.logical.LogicalRootNode;
import nta.engine.planner.logical.ProjectionNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.planner.logical.SelectionNode;
import nta.engine.planner.logical.SortNode;
import nta.engine.planner.logical.UnaryNode;
import nta.engine.planner.logical.UnionNode;
import nta.engine.query.exception.InvalidQueryException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * This class optimizes a logical plan corresponding to one query block.
 * 
 * @author Hyunsik Choi
 *
 */
public class LogicalOptimizer {
  private static Log LOG = LogFactory.getLog(LogicalOptimizer.class);
  
  private LogicalOptimizer() {
  }
  
  public static LogicalNode optimize(Context ctx, LogicalNode plan) {
    LogicalNode toBeOptimized;
    try {
      toBeOptimized = (LogicalNode) plan.clone();
    } catch (CloneNotSupportedException e) {
      LOG.error(e);
      throw new InvalidQueryException("Cannot clone: " + plan);
    }
    
    switch (ctx.getStatementType()) {
    case SELECT:
    //case UNION: // TODO - to be implemented
    //case EXCEPT:
    //case INTERSECT:
    case CREATE_TABLE:
      // if there are selection node 
      if(PlannerUtil.findTopNode(plan, ExprType.SELECTION) != null) {
        pushSelection(ctx, toBeOptimized);
      }
      
      pushProjection(ctx, toBeOptimized);
      
      break;
    default:
    }
    
    return toBeOptimized;
  }
  
  /**
   * This method pushes down the projection list into the appropriate and 
   * below logical operators.
   * @param ctx
   * @param plan
   */
  private static void pushProjection(Context ctx, LogicalNode plan) {
    OutSchemaRefresher out = new OutSchemaRefresher();
    plan.preOrder(out);
    InSchemaRefresher refresher = new InSchemaRefresher();
    plan.postOrder(refresher);

    // TODO - Only if the projection all is pushed down, the projection ca be removed.
    LogicalNode parent = PlannerUtil.
        findTopParentNode(plan, ExprType.PROJECTION);
    if (parent != null) {
      PlannerUtil.deleteNode(parent);
    }
  }
  
  /**
   * This method pushes down the selection into the appropriate sub 
   * logical operators.
   * <br />
   * 
   * There are three operators that can have search conditions.
   * Selection, Join, and GroupBy clause can have search conditions.
   * However, the search conditions of Join and GroupBy cannot be pushed down 
   * into child operators because they can be used when the data layout change
   * caused by join and grouping relations.
   * <br />
   * 
   * However, some of the search conditions of selection clause can be pushed 
   * down into appropriate sub operators. Some comparison expressions on 
   * multiple relations are actually join conditions, and other expression 
   * on single relation can be used in a scan operator or an Index Scan 
   * operator.   
   * 
   * @param ctx
   * @param plan
   */
  private static void pushSelection(Context ctx, LogicalNode plan) {
    SelectionNode selNode = (SelectionNode) PlannerUtil.findTopNode(plan, 
        ExprType.SELECTION);
    Preconditions.checkNotNull(selNode);
    
    Stack<LogicalNode> stack = new Stack<LogicalNode>();
    EvalNode [] cnfs = EvalTreeUtil.getConjNormalForm(selNode.getQual());
    pushSelectionRecursive(ctx, plan, Lists.newArrayList(cnfs), stack);
  }
  
  private static void pushSelectionRecursive(Context ctx, LogicalNode plan,
      List<EvalNode> evalTrees, Stack<LogicalNode> stack) {
    
    switch(plan.getType()) {
    
    case SELECTION:
      SelectionNode selNode = (SelectionNode) plan;
      stack.push(selNode);
      pushSelectionRecursive(ctx, selNode.getSubNode(),
          evalTrees, stack);
      stack.pop();
      
      // remove the selection operator if there is no search condition 
      // after selection push.
      if(evalTrees.size() == 0) {
        LogicalNode node = stack.peek();
        if (node instanceof UnaryNode) {
          UnaryNode unary = (UnaryNode) node;
          unary.setSubNode(selNode.getSubNode());
        } else {
          throw new InvalidQueryException("Unexpected Logical Query Plan");
        }
      }
      break;
    case SCAN:
    case JOIN:      
      List<EvalNode> matched = Lists.newArrayList();
      for (EvalNode eval : evalTrees) {
        if (selectionPushable(eval, plan)) {
          matched.add(eval);
        }
      }
      EvalNode qual;
      if (matched.size() > 1) {
        qual = EvalTreeUtil.transformCNF2Singleton(
            matched.toArray(new EvalNode [matched.size()]));
      } else {
        qual = matched.get(0);
      }
      
      if (plan.getType() == ExprType.SCAN) {
        ScanNode scanNode = (ScanNode) plan;
        scanNode.setQual(qual);
      } else if (plan.getType() == ExprType.JOIN) {
        JoinNode joinNode = (JoinNode) plan;
        if (joinNode.hasJoinQual()) {
          EvalNode conjQual = EvalTreeUtil.
              transformCNF2Singleton(joinNode.getJoinQual(), qual);
          joinNode.setJoinQual(conjQual);
        } else {
          joinNode.setJoinQual(qual);
        }
      }      
      evalTrees.removeAll(matched);      
      break;
      
    default:
      stack.push(plan);
      if (plan instanceof UnaryNode) {
        UnaryNode unary = (UnaryNode) plan;
        pushSelectionRecursive(ctx, unary.getSubNode(), evalTrees, stack);
      } else if (plan instanceof BinaryNode) {
        BinaryNode binary = (BinaryNode) plan;
        pushSelectionRecursive(ctx, binary.getOuterNode(), evalTrees, stack);
        pushSelectionRecursive(ctx, binary.getInnerNode(), evalTrees, stack);
      }
      stack.pop();
      break;
    }
  }
  
  public static boolean selectionPushable(EvalNode eval, LogicalNode node) {
    Set<Column> columnRefs = EvalTreeUtil.findDistinctRefColumns(eval);
    if (node.getType() == ExprType.SCAN) {
      ScanNode scanNode = (ScanNode) node;
      String tableId = scanNode.getTableId();
      for (Column col : columnRefs) {
        if (!col.getTableName().equals(tableId)) {
          return false;
        }
      }    
      return true;
    } else if (node.getType() == ExprType.JOIN) {
      JoinNode joinNode = (JoinNode) node;
      Set<String> tableIds = Sets.newHashSet();
      // getting distinct table references
      for (Column col : columnRefs) {
        if (!tableIds.contains(col.getTableName())) {
          tableIds.add(col.getTableName());
        }
      }
      
      // if the references only indicate two relation, the condition can be 
      // pushed into a join operator.
      if (tableIds.size() != 2) {
        return false;
      }
      
      String outer = PlannerUtil.getLineage(joinNode.getOuterNode());
      String inner = PlannerUtil.getLineage(joinNode.getInnerNode());
      if (outer == null || inner == null) {      
        throw new InvalidQueryException("ERROR: Unexpected logical plan");
      }
      Iterator<String> it = tableIds.iterator();
      if (it.next().equals(outer) && it.next().equals(inner)) {
        return true;
      }
      
      it = tableIds.iterator();
      if (it.next().equals(inner) && it.next().equals(outer)) {
        return true;
      }
      
      return false;
    } else {
      return false;
    }
  }
  
  /**
   * This is designed for the post order.
   */
  public static class OutSchemaRefresher implements LogicalNodeVisitor {
    private final Set<Column> necessary;
    
    public OutSchemaRefresher() {
      this.necessary = Sets.newHashSet();
    }
    
    public Set<Column> getTargetList() {
      return this.necessary;
    }
    
    @Override
    public void visit(LogicalNode node) {
      Set<Column> temp;
      Schema projected;
      switch (node.getType()) {
      case ROOT:
        
        break;
      case PROJECTION:
        ProjectionNode projNode = (ProjectionNode) node;
        for (Target t : projNode.getTargetList()) {
            temp = EvalTreeUtil.findDistinctRefColumns(t.getEvalTree());
            if (!temp.isEmpty()) {
              necessary.addAll(temp);
            }
        }

        break;

      case SELECTION:
        SelectionNode selNode = (SelectionNode) node;
        projected = new Schema();
        for(Column col : selNode.getInputSchema().getColumns()) {
          if(necessary.contains(col)) {
            projected.addColumn(col);
          }
        }        
        selNode.setOutputSchema(projected);
        
        temp = EvalTreeUtil.findDistinctRefColumns(selNode.getQual());
        if (!temp.isEmpty()) {
          necessary.addAll(temp);
        }

        break;
        
      case GROUP_BY:
        GroupbyNode groupByNode = (GroupbyNode)node;
        necessary.addAll(Lists.newArrayList(groupByNode.getGroupingColumns()));
        for (Target t : groupByNode.getTargetList()) {
          temp = EvalTreeUtil.findDistinctRefColumns(t.getEvalTree());
          if (!temp.isEmpty()) {
            necessary.addAll(temp);
          }
        }
        
        if(groupByNode.hasHavingCondition()) {
          temp = EvalTreeUtil.findDistinctRefColumns(groupByNode.
              getHavingCondition());
          if (!temp.isEmpty()) {
            necessary.addAll(temp);
          }
        }
        
        break;
        
      case SORT:
        SortNode sortNode = (SortNode) node;
        projected = new Schema();
        for(Column col : sortNode.getInputSchema().getColumns()) {
          if(necessary.contains(col)) {
            projected.addColumn(col);
          }
        }
        
        sortNode.setOutputSchema(projected);
        for (SortSpec key : sortNode.getSortKeys()) {
          necessary.add(key.getSortKey());
        }
        
        break;
        
      case JOIN:
        JoinNode joinNode = (JoinNode) node;
        projected = new Schema();
        for(Column col : joinNode.getInputSchema().getColumns()) {
          if(necessary.contains(col)) {
            projected.addColumn(col);
          }
        }
        joinNode.setOutputSchema(projected);
        
        if (joinNode.hasJoinQual()) {
          temp = EvalTreeUtil.findDistinctRefColumns(joinNode.getJoinQual());
          if (!temp.isEmpty()) {
            necessary.addAll(temp);
          }
        }
        
        break;
        
      case UNION:
        UnionNode unionNode = (UnionNode) node;
        projected = new Schema();
        for(Column col : unionNode.getInputSchema().getColumns()) {
          if(necessary.contains(col)) {
            projected.addColumn(col);
          }
        }
        unionNode.setOutputSchema(projected);
        break;
        
      case SCAN:
        ScanNode scanNode = (ScanNode) node;
        projected = new Schema();
        for(Column col : scanNode.getInputSchema().getColumns()) {
          if(necessary.contains(col)) {
            projected.addColumn(col);
          }
        }
        scanNode.setOutputSchema(projected);
        
        if (scanNode.hasQual()) {
          temp = EvalTreeUtil.findDistinctRefColumns(scanNode.getQual());
          if (!temp.isEmpty()) {
            necessary.addAll(temp);
          }
        }

        break;
        
      default:
      }
    }
  }
  
  public static class InSchemaRefresher implements LogicalNodeVisitor {   
    public InSchemaRefresher() {
    }
    
    @Override
    public void visit(LogicalNode node) {
      switch (node.getType()) {
      case SCAN:        
      break;
      
      case JOIN:
        BinaryNode join = (BinaryNode) node;
        Schema merged = SchemaUtil.merge(join.getOuterNode().getOutputSchema(),
            join.getInnerNode().getOutputSchema());
        join.setInputSchema(merged);
        break;
        
      case UNION:
        UnionNode union = (UnionNode) node;        
        union.setInputSchema(union.getOuterNode().getOutputSchema());
        break;
      
      default:
        if (node instanceof UnaryNode) {
          UnaryNode unary = (UnaryNode) node;
          unary.setInputSchema(unary.getSubNode().getOutputSchema());
        }
      }
    }
  }
  
  public static void pushProjection(Context ctx,
      LogicalNode logicalNode, Set<Column> necessary, 
      Stack<LogicalNode> stack) {
    
    if (logicalNode == null) {
      return;
    }
    
    Schema inputSchema;
    Schema outputSchema;
    
    switch(logicalNode.getType()) {
    case ROOT:
      LogicalRootNode root = (LogicalRootNode) logicalNode;
      stack.push(root);
      pushProjection(ctx, root.getSubNode(), necessary, stack);
      stack.pop();
      
      root.setSubNode(root.getSubNode());
      break;
    
    case STORE:
      StoreTableNode storeNode = (StoreTableNode) logicalNode;
      stack.push(storeNode);
      pushProjection(ctx, storeNode.getSubNode(), necessary, stack);
      stack.pop();
      storeNode.setSubNode(storeNode.getSubNode());
      break;
      
    case PROJECTION:
      ProjectionNode projNode = ((ProjectionNode)logicalNode);
      if(necessary != null) {
        for(Target t : projNode.getTargetList()) {
            getTargetListFromEvalTree(projNode.getInputSchema(), t.getEvalTree(), 
                necessary);
        }
        
        stack.push(projNode);
        pushProjection(ctx, projNode.getSubNode(), necessary, stack);
        stack.pop();
        
        LogicalNode parent = stack.peek();
        if(parent instanceof UnaryNode) {
          ((UnaryNode) parent).setSubNode((projNode).getSubNode());
        } else {
          throw new InvalidQueryException("Unexpected Logical Query Plan");
        }
      } else {
        stack.push(projNode);
        pushProjection(ctx, projNode.getSubNode(), necessary, stack);
        stack.pop();
      }
      
      if (projNode.getSubNode() != null)
        projNode.setInputSchema(projNode.getSubNode().getOutputSchema());
      
      Schema prjTargets = new Schema();
      for(Target t : projNode.getTargetList()) {
        DataType type = t.getEvalTree().getValueType();
        String name = t.getEvalTree().getName();
        prjTargets.addColumn(name,type);
      }
      projNode.setOutputSchema(prjTargets);
      
      break;
      
    case SELECTION:
      SelectionNode selNode = ((SelectionNode)logicalNode);
      if(necessary != null) { // optimization phase
        getTargetListFromEvalTree(selNode.getInputSchema(), selNode.getQual(), 
            necessary);
      }
      stack.push(selNode);
      pushProjection(ctx, selNode.getSubNode(), necessary, stack);
      stack.pop();
      inputSchema = selNode.getSubNode().getOutputSchema();
      selNode.setInputSchema(inputSchema);
      selNode.setOutputSchema(inputSchema);
      
      break;
      
    case GROUP_BY:
      GroupbyNode groupByNode = ((GroupbyNode)logicalNode);
      
      if(necessary != null) { // projection push phase
        if(groupByNode.hasHavingCondition()) {
          getTargetListFromEvalTree(groupByNode.getInputSchema(),
              groupByNode.getHavingCondition(), necessary);
        }
        
        for(Column grpField : groupByNode.getGroupingColumns()) {
          necessary.add(grpField);
        }
        
/*        Target [] grpTargetList = null;
        for(LogicalNode node : stack) {
          if(node.getType() == ExprType.PROJECTION) {
            ProjectionNode prjNode = (ProjectionNode) node;
            grpTargetList = prjNode.getTargetList();
          }
        }
        if(grpTargetList != null) {
          groupByNode.setTargetList(grpTargetList);
        }*/
        
        for (Target t : groupByNode.getTargetList()) {
          getTargetListFromEvalTree(groupByNode.getInputSchema(),
              t.getEvalTree(), necessary);
        }
      }
      stack.push(groupByNode);
      pushProjection(ctx, groupByNode.getSubNode(), necessary, stack);
      stack.pop();
      groupByNode.setInputSchema(groupByNode.getSubNode().getOutputSchema());
      
      Schema grpTargets = new Schema();
      for(Target t : ctx.getTargetList()) {
        DataType type = t.getEvalTree().getValueType();
        String name = t.getEvalTree().getName();
        grpTargets.addColumn(name,type);
      }
      groupByNode.setTargetList(ctx.getTargetList());
      groupByNode.setOutputSchema(grpTargets);
      
      break;
      
    case SCAN:
      ScanNode scanNode = ((ScanNode)logicalNode);
      Schema scanSchema = 
          ctx.getTable(scanNode.getTableId()).getMeta().getSchema();
      Schema scanTargetList = new Schema();
      scanTargetList.addColumns(scanSchema);
      scanNode.setInputSchema(scanTargetList);
      
      if(necessary != null) { // projection push phase
        outputSchema = new Schema();
        for(Column col : scanTargetList.getColumns()) {
          if(necessary.contains(col)) {
            outputSchema.addColumn(col);
          }
        }
        scanNode.setOutputSchema(outputSchema);
        
        Schema projectedList = new Schema();
        if(scanNode.hasQual()) {
          getTargetListFromEvalTree(scanTargetList, scanNode.getQual(), 
              necessary);
        }
        for(Column col : scanTargetList.getColumns()) {
          if(necessary.contains(col)) {
            projectedList.addColumn(col);
          }
        }
        
        scanNode.setTargetList(projectedList);
      } else {
        scanNode.setOutputSchema(scanTargetList);
      }
      
      break;
      
    case SORT:
      SortNode sortNode = ((SortNode)logicalNode);
      // TODO - before this, should modify sort keys to eval trees.
      stack.push(sortNode);
      pushProjection(ctx, sortNode.getSubNode(), necessary, stack);
      stack.pop();
      inputSchema = sortNode.getSubNode().getOutputSchema();
      sortNode.setInputSchema(inputSchema);
      sortNode.setOutputSchema(inputSchema);
      
      break;
    
    case JOIN:
      JoinNode joinNode = (JoinNode) logicalNode;
      stack.push(joinNode);
      pushProjection(ctx, joinNode.getOuterNode(), necessary, 
          stack);
      pushProjection(ctx, joinNode.getInnerNode(), necessary,
          stack);
      stack.pop();      
            
      break;
      default:;
    }
  }
  
  static void getTargetListFromEvalTree(Schema inputSchema, 
      EvalNode evalTree, Set<Column> targetList) {
    
    switch(evalTree.getType()) {
    case FIELD:
      FieldEval fieldEval = (FieldEval) evalTree;
      Column col = inputSchema.getColumn(fieldEval.getName());
      targetList.add(col);
      
      break;
    
    case PLUS:
    case MINUS:
    case MULTIPLY:
    case DIVIDE:
    case AND:
    case OR:    
    case EQUAL:
    case NOT_EQUAL:
    case LTH:
    case LEQ:
    case GTH:   
    case GEQ:
      getTargetListFromEvalTree(inputSchema, evalTree.getLeftExpr(), targetList);
      getTargetListFromEvalTree(inputSchema, evalTree.getRightExpr(), targetList);
      
      break;
     case FUNCTION:
       FuncCallEval funcEval = (FuncCallEval) evalTree;
       for(EvalNode evalNode : funcEval.getGivenArgs()) {
         getTargetListFromEvalTree(inputSchema, evalNode, targetList);
       }
    default:;
    }
  }
}