/**
 * 
 */
package nta.engine.planner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.SchemaUtil;
import nta.engine.Context;
import nta.engine.exec.eval.EvalNode;
import nta.engine.exec.eval.EvalTreeUtil;
import nta.engine.parser.QueryBlock.SortSpec;
import nta.engine.parser.QueryBlock.Target;
import nta.engine.planner.logical.*;
import nta.engine.query.exception.InvalidQueryException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;

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
    pushDownProjection(ctx, plan);

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
    EvalNode [] cnf = EvalTreeUtil.getConjNormalForm(selNode.getQual());
    pushSelectionRecursive(ctx, plan, Lists.newArrayList(cnf), stack);
  }
  
  private static void pushSelectionRecursive(Context ctx, LogicalNode plan,
      List<EvalNode> cnf, Stack<LogicalNode> stack) {
    
    switch(plan.getType()) {
    
    case SELECTION:
      SelectionNode selNode = (SelectionNode) plan;
      stack.push(selNode);
      pushSelectionRecursive(ctx, selNode.getSubNode(),
          cnf, stack);
      stack.pop();
      
      // remove the selection operator if there is no search condition 
      // after selection push.
      if(cnf.size() == 0) {
        LogicalNode node = stack.peek();
        if (node instanceof UnaryNode) {
          UnaryNode unary = (UnaryNode) node;
          unary.setSubNode(selNode.getSubNode());
        } else {
          throw new InvalidQueryException("Unexpected Logical Query Plan");
        }
      }
      break;
    case JOIN:
      JoinNode join = (JoinNode) plan;

      LogicalNode outer = join.getOuterNode();
      LogicalNode inner = join.getInnerNode();

      pushSelectionRecursive(ctx, outer, cnf, stack);
      pushSelectionRecursive(ctx, inner, cnf, stack);

      List<EvalNode> matched = Lists.newArrayList();
      for (EvalNode eval : cnf) {
        if (selectionPushable(eval, plan)) {
          matched.add(eval);
        }
      }

      EvalNode qual = null;
      if (matched.size() > 1) {
        // merged into one eval tree
        qual = EvalTreeUtil.transformCNF2Singleton(
            matched.toArray(new EvalNode [matched.size()]));
      } else if (matched.size() == 1) {
        // if the number of matched expr is one
        qual = matched.get(0);
      }

      if (qual != null) {
        JoinNode joinNode = (JoinNode) plan;
        if (joinNode.hasJoinQual()) {
          EvalNode conjQual = EvalTreeUtil.
              transformCNF2Singleton(joinNode.getJoinQual(), qual);
          joinNode.setJoinQual(conjQual);
        } else {
          joinNode.setJoinQual(qual);
        }
        cnf.removeAll(matched);
      }

      break;

    case SCAN:
      matched = Lists.newArrayList();
      for (EvalNode eval : cnf) {
        if (selectionPushable(eval, plan)) {
          matched.add(eval);
        }
      }

      qual = null;
      if (matched.size() > 1) {
        // merged into one eval tree
        qual = EvalTreeUtil.transformCNF2Singleton(
            matched.toArray(new EvalNode [matched.size()]));
      } else if (matched.size() == 1) {
        // if the number of matched expr is one
        qual = matched.get(0);
      }

      if (qual != null) { // if a matched qual exists
        ScanNode scanNode = (ScanNode) plan;
        scanNode.setQual(qual);
      }

      cnf.removeAll(matched);
      break;

    default:
      stack.push(plan);
      if (plan instanceof UnaryNode) {
        UnaryNode unary = (UnaryNode) plan;
        pushSelectionRecursive(ctx, unary.getSubNode(), cnf, stack);
      } else if (plan instanceof BinaryNode) {
        BinaryNode binary = (BinaryNode) plan;
        pushSelectionRecursive(ctx, binary.getOuterNode(), cnf, stack);
        pushSelectionRecursive(ctx, binary.getInnerNode(), cnf, stack);
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
      
      String [] outer = PlannerUtil.getLineage(joinNode.getOuterNode());
      String [] inner = PlannerUtil.getLineage(joinNode.getInnerNode());

      Set<String> o = Sets.newHashSet(outer);
      Set<String> i = Sets.newHashSet(inner);
      if (outer == null || inner == null) {      
        throw new InvalidQueryException("ERROR: Unexpected logical plan");
      }
      Iterator<String> it = tableIds.iterator();
      if (o.contains(it.next()) && i.contains(it.next())) {
        return true;
      }
      
      it = tableIds.iterator();
      if (i.contains(it.next()) && o.contains(it.next())) {
        return true;
      }
      
      return false;
    } else {
      return false;
    }
  }

  public static LogicalNode pushDownProjection(Context ctx, LogicalNode plan) {
    Set<Column> set = Sets.newHashSet();
   return shrinkSchema(ctx, plan, set, false);
  }

  /**
   * Groupby, Join, and Scan can project necessary columns.
   * This method has three roles:
   * 1) collect column reference necessary for sortkeys, join keys, selection conditions, grouping fields,
   * and having conditions
   * 2) shrink the output schema of each operator so that the operator reduces the output columns according to
   * the necessary columns of their parent operators
   * 3) shrink the input schema of each operator according to the shrunk output schemas of the child operators.
   *
   *
   * @param ctx
   * @param node
   * @param necessary
   * @param projected
   * @return
   */
  private static LogicalNode shrinkSchema(Context ctx, LogicalNode node, Set<Column> necessary, boolean projected) {
    Set<Column> temp;
    LogicalNode outer;
    LogicalNode inner;
    switch (node.getType()) {
      case ROOT: // It does not support the projection
        LogicalRootNode root = (LogicalRootNode) node;
        outer = shrinkSchema(ctx, root.getSubNode(), necessary, projected);
        root.setInputSchema(outer.getOutputSchema());
        root.setOutputSchema(outer.getOutputSchema());
        break;


      case PROJECTION:
        ProjectionNode projNode = (ProjectionNode) node;
        for (Target t : projNode.getTargetList()) {
          temp = EvalTreeUtil.findDistinctRefColumns(t.getEvalTree());
          if (!temp.isEmpty()) {
            necessary.addAll(temp);
          }
        }

        outer = shrinkSchema(ctx, projNode.getSubNode(), necessary, projected);
        projNode.setInputSchema(outer.getOutputSchema());
        break;

      case SELECTION: // It does not support the projection
        SelectionNode selNode = (SelectionNode) node;
        // because selection does not support the projection
        //selNode.setOutputSchema(shrinkOutSchema(selNode.getInputSchema(), necessary));

        temp = EvalTreeUtil.findDistinctRefColumns(selNode.getQual());
        if (!temp.isEmpty()) {
          necessary.addAll(temp);
        }

        outer = shrinkSchema(ctx, selNode.getSubNode(), necessary, projected);
        selNode.setInputSchema(outer.getOutputSchema());
        selNode.setOutputSchema(outer.getOutputSchema());
        break;

      case GROUP_BY:
        GroupbyNode groupByNode = (GroupbyNode)node;
        // TODO - to be improved

        necessary.addAll(Lists.newArrayList(groupByNode.getGroupingColumns()));
        for (Target t : groupByNode.getTargetList()) {
          temp = EvalTreeUtil.findDistinctRefColumns(t.getEvalTree());
          if (!temp.isEmpty()) {
            necessary.addAll(temp);
          }
        }

        // it guarantees that the the top operator of the logical operator tree
        // serves as the projection node.
        if (!projected) {
          groupByNode.setOutputSchema(
              LogicalPlanner.getProjectedSchema(ctx, ctx.getTargetList()));
        }

        if(groupByNode.hasHavingCondition()) {
          temp = EvalTreeUtil.findDistinctRefColumns(groupByNode.
              getHavingCondition());
          if (!temp.isEmpty()) {
            necessary.addAll(temp);
          }
        }
        outer = shrinkSchema(ctx, groupByNode.getSubNode(), necessary, true);
        groupByNode.setInputSchema(outer.getOutputSchema());
        break;

      case SORT: // It does not support the projection
        SortNode sortNode = (SortNode) node;
        for (SortSpec key : sortNode.getSortKeys()) {
          necessary.add(key.getSortKey());
        }

        outer = shrinkSchema(ctx, sortNode.getSubNode(), necessary, projected);
        sortNode.setInputSchema(outer.getOutputSchema());
        sortNode.setOutputSchema(outer.getOutputSchema());
        break;


      case JOIN:
        JoinNode joinNode = (JoinNode) node;

        if (!projected) {
          // it guarantees that the the top operator of the logical operator tree
          // serves as the projection node.
          joinNode.setOutputSchema(LogicalPlanner.getProjectedSchema(ctx, ctx.getTargetList()));
        } else {
          joinNode.setOutputSchema(shrinkOutSchema(joinNode.getInputSchema(), necessary));
        }
        if (joinNode.hasJoinQual()) {
          temp = EvalTreeUtil.findDistinctRefColumns(joinNode.getJoinQual());
          if (!temp.isEmpty()) {
            necessary.addAll(temp);
          }
        }

        outer = shrinkSchema(ctx, joinNode.getOuterNode(), necessary, true);
        inner = shrinkSchema(ctx, joinNode.getInnerNode(), necessary, true);
        Schema merged = SchemaUtil.merge(outer.getOutputSchema(), inner.getOutputSchema());
        joinNode.setInputSchema(merged);
        break;

      case UNION:  // It does not support the projection
        UnionNode unionNode = (UnionNode) node;
        shrinkSchema(ctx, unionNode.getOuterNode(), necessary, projected);
        shrinkSchema(ctx, unionNode.getInnerNode(), necessary, projected);
        break;

      case SCAN:
        ScanNode scanNode = (ScanNode) node;
        if (!projected) {
          // it guarantees that the the top operator of the logical operator tree
          // serves as the projection node.
          scanNode.setOutputSchema(LogicalPlanner.getProjectedSchema(ctx, ctx.getTargetList()));
        } else {
          scanNode.setOutputSchema(shrinkOutSchema(scanNode.getInputSchema(), necessary));
        }

        if (scanNode.hasQual()) {
          temp = EvalTreeUtil.findDistinctRefColumns(scanNode.getQual());
          if (!temp.isEmpty()) {
            necessary.addAll(temp);
          }
        }
        break;

      default:
    }

    return node;
  }

  private static Schema shrinkOutSchema(Schema inSchema, Set<Column> necessary) {
    Schema projected = new Schema();
    for(Column col : inSchema.getColumns()) {
      if(necessary.contains(col)) {
        projected.addColumn(col);
      }
    }
    return projected;
  }
}