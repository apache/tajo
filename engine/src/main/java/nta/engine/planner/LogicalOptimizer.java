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
import nta.engine.exec.eval.FieldEval;
import nta.engine.parser.QueryBlock;
import nta.engine.parser.QueryBlock.Target;
import nta.engine.planner.logical.*;
import nta.engine.query.exception.InvalidQueryException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

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

      try {
        pushProjection(ctx, toBeOptimized);
      } catch (CloneNotSupportedException e) {
        throw new InvalidQueryException(e);
      }

      break;
    default:
    }
    
    return toBeOptimized;
  }
  
  /**
   * This method pushes down the selection into the appropriate sub 
   * logical operators.
   * <br />
   * 
   * There are three operators that can have search conditions.
   * Selection, Join, and GroupBy clause.
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
        if (canBeEvaluated(eval, plan)) {
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
        if (joinNode.getJoinType() == JoinType.CROSS_JOIN) {
          joinNode.setJoinType(JoinType.INNER);
        }
        cnf.removeAll(matched);
      }

      break;

    case SCAN:
      matched = Lists.newArrayList();
      for (EvalNode eval : cnf) {
        if (canBeEvaluated(eval, plan)) {
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
  
  public static boolean canBeEvaluated(EvalNode eval, LogicalNode node) {
    Set<Column> columnRefs = EvalTreeUtil.findDistinctRefColumns(eval);

    if (node.getType() == ExprType.JOIN) {
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
      for (Column col : columnRefs) {
        if (!node.getInputSchema().contains(col.getQualifiedName())) {
          return false;
        }
      }

      return true;
    }
  }

  /**
   * This method pushes down the projection list into the appropriate and
   * below logical operators.
   * @param ctx
   * @param plan
   */
  private static void pushProjection(Context ctx, LogicalNode plan) throws CloneNotSupportedException {
    Stack<LogicalNode> stack = new Stack<LogicalNode>();
    OptimizationContext optCtx = new OptimizationContext(ctx);
    pushProjectionRecursive(optCtx, plan, stack, new HashSet<Column>());
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
   * //@param necessary - columns necessary for above logical nodes, but it excepts the fields for the target list
   * //@param targetList
   * @return
   */
  private static LogicalNode pushProjectionRecursive(OptimizationContext ctx, LogicalNode node, Stack<LogicalNode> stack,
                                                     Set<Column> necessary) throws CloneNotSupportedException {
    LogicalNode outer;
    LogicalNode inner;
    switch (node.getType()) {
      case ROOT: // It does not support the projection
        LogicalRootNode root = (LogicalRootNode) node;
        stack.add(root);
        outer = pushProjectionRecursive(ctx, root.getSubNode(), stack, necessary);
        root.setInputSchema(outer.getOutputSchema());
        root.setOutputSchema(outer.getOutputSchema());
        break;

      case STORE:
        StoreTableNode store = (StoreTableNode) node;
        stack.add(store);
        outer = pushProjectionRecursive(ctx, store.getSubNode(), stack, necessary);
        store.setInputSchema(outer.getOutputSchema());
        store.setOutputSchema(outer.getOutputSchema());
        break;

      case PROJECTION:
        ProjectionNode projNode = (ProjectionNode) node;

        stack.add(projNode);
        outer = pushProjectionRecursive(ctx, projNode.getSubNode(), stack, necessary);
        stack.pop();

        LogicalNode childNode = projNode.getSubNode();
        if (ctx.getTargetListManager().isAllEvaluated() // if all exprs are evaluated
            && (childNode.getType() == ExprType.JOIN ||
               childNode.getType() == ExprType.GROUP_BY ||
               childNode.getType() == ExprType.SCAN)) { // if the child node is projectable
            projNode.getSubNode().setOutputSchema(ctx.getTargetListManager().getUpdatedSchema());
            LogicalNode parent = stack.peek();
            ((UnaryNode)parent).setSubNode(projNode.getSubNode());
            return projNode.getSubNode();
        } else {
          // the output schema is not changed.
          projNode.setInputSchema(outer.getOutputSchema());
          projNode.setTargetList(ctx.getTargetListManager().getUpdatedTarget());
        }
        return projNode;

      case SELECTION: // It does not support the projection
        SelectionNode selNode = (SelectionNode) node;

        if (selNode.getQual() != null) {
          necessary.addAll(EvalTreeUtil.findDistinctRefColumns(selNode.getQual()));
        }

        stack.add(selNode);
        outer = pushProjectionRecursive(ctx, selNode.getSubNode(), stack, necessary);
        stack.pop();
        selNode.setInputSchema(outer.getOutputSchema());
        selNode.setOutputSchema(outer.getOutputSchema());
        break;

      case GROUP_BY: {
        GroupbyNode groupByNode = (GroupbyNode)node;

        if (groupByNode.hasHavingCondition()) {
          necessary.addAll(EvalTreeUtil.findDistinctRefColumns(groupByNode.getHavingCondition()));
        }

        stack.add(groupByNode);
        outer = pushProjectionRecursive(ctx, groupByNode.getSubNode(), stack, necessary);
        stack.pop();
        groupByNode.setInputSchema(outer.getOutputSchema());
        // set all targets
        groupByNode.setTargetList(ctx.getTargetListManager().getUpdatedTarget());

        TargetListManager targets = ctx.getTargetListManager();
        List<Target> groupbyPushable = Lists.newArrayList();
        List<Integer> groupbyPushableId = Lists.newArrayList();

        EvalNode expr;
        for (int i = 0; i < targets.size(); i++) {
          expr = targets.getTarget(i).getEvalTree();
          if (canBeEvaluated(expr, groupByNode) &&
              EvalTreeUtil.findDistinctAggFunction(expr).size() > 0 && expr.getType() != EvalNode.Type.FIELD) {
            targets.setEvaluated(i);
            groupbyPushable.add((Target) targets.getTarget(i).clone());
            groupbyPushableId.add(i);
          }
        }

        return groupByNode;
      }

      case SORT: // It does not support the projection
        SortNode sortNode = (SortNode) node;

        for (QueryBlock.SortSpec spec : sortNode.getSortKeys()) {
          necessary.add(spec.getSortKey());
        }

        stack.add(sortNode);
        outer = pushProjectionRecursive(ctx, sortNode.getSubNode(), stack, necessary);
        stack.pop();
        sortNode.setInputSchema(outer.getOutputSchema());
        sortNode.setOutputSchema(outer.getOutputSchema());
        break;


      case JOIN: {
        JoinNode joinNode = (JoinNode) node;
        Set<Column> parentNecessary = Sets.newHashSet(necessary);

        if (joinNode.hasJoinQual()) {
          necessary.addAll(EvalTreeUtil.findDistinctRefColumns(joinNode.getJoinQual()));
        }

        stack.add(joinNode);
        outer = pushProjectionRecursive(ctx, joinNode.getOuterNode(), stack, necessary);
        inner = pushProjectionRecursive(ctx, joinNode.getInnerNode(), stack, necessary);
        stack.pop();
        Schema merged = SchemaUtil.merge(outer.getOutputSchema(), inner.getOutputSchema());
        joinNode.setInputSchema(merged);

        TargetListManager targets = ctx.getTargetListManager();
        List<Target> joinPushable = Lists.newArrayList();
        List<Integer> joinPushableId = Lists.newArrayList();
        EvalNode expr;
        for (int i = 0; i < targets.size(); i++) {
          expr = targets.getTarget(i).getEvalTree();
          if (canBeEvaluated(expr, joinNode)
              && EvalTreeUtil.findDistinctAggFunction(expr).size() == 0
              && expr.getType() != EvalNode.Type.FIELD) {
            targets.setEvaluated(i);
            joinPushable.add(targets.getTarget(i));
            joinPushableId.add(i);
          }
        }
        if (joinPushable.size() > 0) {
          joinNode.setTargetList(targets.targets);
        }

        Schema outSchema = shrinkOutSchema(joinNode.getInputSchema(), targets.getUpdatedSchema().getColumns());
        for (Integer t : joinPushableId) {
          outSchema.addColumn(targets.getEvaluatedColumn(t));
        }
        outSchema = SchemaUtil.mergeAllWithNoDup(outSchema.getColumns(),
            SchemaUtil.getProjectedSchema(joinNode.getInputSchema(),parentNecessary).getColumns());
        joinNode.setOutputSchema(outSchema);
        break;
      }

      case UNION:  // It does not support the projection
        UnionNode unionNode = (UnionNode) node;
        stack.add(unionNode);
        OptimizationContext outerCtx = new OptimizationContext(ctx.ctx);
        OptimizationContext innerCtx = new OptimizationContext(ctx.ctx);
        pushProjectionRecursive(outerCtx, unionNode.getOuterNode(), stack, necessary);
        pushProjectionRecursive(innerCtx, unionNode.getInnerNode(), stack, necessary);
        stack.pop();

        // if this is the final union, we assume that all targets are evalauted
        // TODO - is it always correct?
        if (stack.peek().getType() != ExprType.UNION) {
          ctx.getTargetListManager().setEvaluatedAll();
        }
        break;

      case SCAN: {
        ScanNode scanNode = (ScanNode) node;
        TargetListManager targets = ctx.getTargetListManager();
        List<Integer> scanPushableId = Lists.newArrayList();
        List<Target> scanPushable = Lists.newArrayList();
        EvalNode expr;
        for (int i = 0; i < targets.size(); i++) {
          expr = targets.getTarget(i).getEvalTree();
          if (!targets.isEvaluated(i) && canBeEvaluated(expr, scanNode)) {
            if (expr.getType() == EvalNode.Type.FIELD) {
              targets.setEvaluated(i);
            } else if (EvalTreeUtil.findDistinctAggFunction(expr).size() == 0) {
              targets.setEvaluated(i);
              scanPushable.add(targets.getTarget(i));
              scanPushableId.add(i);
            }
          }
        }

        if (scanPushable.size() > 0) {
          scanNode.setTargets(scanPushable.toArray(new Target[scanPushable.size()]));
        }
        Schema outSchema = shrinkOutSchema(scanNode.getInputSchema(), targets.getUpdatedSchema().getColumns());
        for (Integer t : scanPushableId) {
          outSchema.addColumn(targets.getEvaluatedColumn(t));
        }
        outSchema = SchemaUtil.mergeAllWithNoDup(outSchema.getColumns(), SchemaUtil.getProjectedSchema(scanNode.getInputSchema(),necessary).getColumns());
        scanNode.setOutputSchema(outSchema);

        break;
      }

      default:
    }

    return node;
  }

  private static Schema shrinkOutSchema(Schema inSchema, Collection<Column> necessary) {
    Schema projected = new Schema();
    for(Column col : inSchema.getColumns()) {
      if(necessary.contains(col)) {
        projected.addColumn(col);
      }
    }
    return projected;
  }

  public static class OptimizationContext {
    Context ctx;
    TargetListManager targetListManager;

    public OptimizationContext(Context ctx) {
      this.ctx = ctx;
      this.targetListManager = new TargetListManager(ctx);
    }

    public TargetListManager getTargetListManager() {
      return this.targetListManager;
    }
  }

  public static class TargetListManager {
    private Context ctx;
    private boolean [] evaluated;
    private Target [] targets;

    public TargetListManager(Context ctx) {
      this.ctx = ctx;
      if (ctx.getTargetList() == null) {
        evaluated = new boolean[0];
      } else {
        evaluated = new boolean[ctx.getTargetList().length];
      }
      this.targets = ctx.getTargetList();
    }

    public Target getTarget(int id) {
      return targets[id];
    }

    public Target [] getTargets() {
      return this.targets;
    }

    public int size() {
      return targets.length;
    }

    public void setEvaluated(int id) {
      evaluated[id] = true;
    }

    public void setEvaluatedAll() {
      for (int i = 0; i < evaluated.length; i++) {
        evaluated[i] = true;
      }
    }

    private boolean isEvaluated(int id) {
      return evaluated[id];
    }

    public Target [] getUpdatedTarget() throws CloneNotSupportedException {
      Target [] updated = new Target[evaluated.length];
      for (int i = 0; i < evaluated.length; i++) {
        if (evaluated[i]) {
          Column col = getEvaluatedColumn(i);
          updated[i] = new Target(new FieldEval(col), i);
        } else {
          updated[i] = (Target) targets[i].clone();
        }
      }
      return updated;
    }

    public Schema getUpdatedSchema() {
      Schema schema = new Schema();
      for (int i = 0; i < evaluated.length; i++) {
        if (evaluated[i]) {
          Column col = getEvaluatedColumn(i);
          schema.addColumn(col);
        } else {
          Collection<Column> cols = getColumnRefs(i);
          for (Column col : cols) {
            if (!schema.contains(col.getQualifiedName())) {
              schema.addColumn(col);
            }
          }
        }
      }

      return schema;
    }

    public Collection<Column> getColumnRefs(int id) {
      return EvalTreeUtil.findDistinctRefColumns(targets[id].getEvalTree());
    }

    public Column getEvaluatedColumn(int id) {
      Target t = targets[id];
      String name;
      if (t.hasAlias()) {
        name = t.getAlias();
      } else if (t.getEvalTree().getName().equals("?")) {
        name = ctx.getUnnamedColumn();
      } else {
        name = t.getEvalTree().getName();
      }
      return new Column(name, t.getEvalTree().getValueType()[0]);
    }

    public boolean isAllEvaluated() {
      for (boolean isEval : evaluated) {
        if (!isEval) {
          return false;
        }
      }

      return true;
    }
  }
}