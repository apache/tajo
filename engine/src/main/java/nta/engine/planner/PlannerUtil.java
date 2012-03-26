package nta.engine.planner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import nta.catalog.Column;
import nta.engine.exec.eval.EvalNode.Type;
import nta.engine.exec.eval.EvalTreeUtil;
import nta.engine.exec.eval.FuncCallEval;
import nta.engine.parser.QueryBlock.SortKey;
import nta.engine.parser.QueryBlock.Target;
import nta.engine.planner.LogicalOptimizer.InSchemaRefresher;
import nta.engine.planner.LogicalOptimizer.OutSchemaRefresher;
import nta.engine.planner.logical.BinaryNode;
import nta.engine.planner.logical.CreateTableNode;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.GroupbyNode;
import nta.engine.planner.logical.JoinNode;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.LogicalNodeVisitor;
import nta.engine.planner.logical.ProjectionNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.planner.logical.SelectionNode;
import nta.engine.planner.logical.SortNode;
import nta.engine.planner.logical.UnaryNode;
import nta.engine.query.exception.InvalidQueryException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * @author Hyunsik Choi
 */
public class PlannerUtil {
  private static final Log LOG = LogFactory.getLog(PlannerUtil.class);
  
  /**
   * Refresh in/out schemas of all level logical nodes from scan nodes to root.
   * This method changes the input logical plan. If you do not want that, you
   * should copy the input logical plan before do it.
   * 
   * @param plan
   * @return
   */
  public static LogicalNode refreshSchema(LogicalNode plan) {    
    OutSchemaRefresher outRefresher = new OutSchemaRefresher();
    plan.preOrder(outRefresher);
    InSchemaRefresher inRefresher = new InSchemaRefresher();
    plan.postOrder(inRefresher);
    
    return plan;
  }
  
  public static String getLineage(LogicalNode node) {
    ScanNode scanNode = (ScanNode) PlannerUtil.findTopNode(node, ExprType.SCAN);
    return scanNode.getTableId();
  }
  
  public static LogicalNode insertNode(LogicalNode parent, LogicalNode newNode) {
    Preconditions.checkArgument(parent instanceof UnaryNode);
    Preconditions.checkArgument(newNode instanceof UnaryNode);
    
    UnaryNode p = (UnaryNode) parent;
    LogicalNode c = p.getSubNode();
    UnaryNode m = (UnaryNode) newNode;
    m.setInputSchema(c.getOutputSchema());
    m.setOutputSchema(c.getOutputSchema());
    m.setSubNode(c);
    p.setSubNode(m);
    
    return p;
  }
  
  /**
   * Delete the child of a given parent operator.
   * 
   * @param parent Must be a unary logical operator.
   * @return
   */
  public static LogicalNode deleteNode(LogicalNode parent) {
    if (parent instanceof UnaryNode) {
      UnaryNode unary = (UnaryNode) parent;
      if (unary.getSubNode() instanceof UnaryNode) {
        UnaryNode child = (UnaryNode) unary.getSubNode();
        LogicalNode grandChild = child.getSubNode();
        unary.setSubNode(grandChild);
      } else {
        throw new InvalidQueryException("Unexpected logical plan: " + parent);
      }
    } else {
      throw new InvalidQueryException("Unexpected logical plan: " + parent);
    }    
    return parent;
  }
  
  public static void replaceNode(LogicalNode plan, LogicalNode newNode, ExprType type) {
    LogicalNode parent = findTopParentNode(plan, type);
    Preconditions.checkArgument(parent instanceof UnaryNode);
    Preconditions.checkArgument(!(newNode instanceof BinaryNode));
    UnaryNode parentNode = (UnaryNode) parent;
    LogicalNode child = parentNode.getSubNode();
    if (child instanceof UnaryNode) {
      ((UnaryNode) newNode).setSubNode(((UnaryNode)child).getSubNode());
    }
    parentNode.setSubNode(newNode);
  }
  
  public static LogicalNode insertOuterNode(LogicalNode parent, LogicalNode outer) {
    Preconditions.checkArgument(parent instanceof BinaryNode);
    Preconditions.checkArgument(outer instanceof UnaryNode);
    
    BinaryNode p = (BinaryNode) parent;
    LogicalNode c = p.getOuterNode();
    UnaryNode m = (UnaryNode) outer;
    m.setInputSchema(c.getOutputSchema());
    m.setOutputSchema(c.getOutputSchema());
    m.setSubNode(c);
    p.setOuter(m);
    return p;
  }
  
  public static LogicalNode insertInnerNode(LogicalNode parent, LogicalNode inner) {
    Preconditions.checkArgument(parent instanceof BinaryNode);
    Preconditions.checkArgument(inner instanceof UnaryNode);
    
    BinaryNode p = (BinaryNode) parent;
    LogicalNode c = p.getInnerNode();
    UnaryNode m = (UnaryNode) inner;
    m.setInputSchema(c.getOutputSchema());
    m.setOutputSchema(c.getOutputSchema());
    m.setSubNode(c);
    p.setInner(m);
    return p;
  }
  
  public static LogicalNode insertNode(LogicalNode parent, 
      LogicalNode left, LogicalNode right) {
    Preconditions.checkArgument(parent instanceof BinaryNode);
    Preconditions.checkArgument(left instanceof UnaryNode);
    Preconditions.checkArgument(right instanceof UnaryNode);
    
    BinaryNode p = (BinaryNode)parent;
    LogicalNode lc = p.getOuterNode();
    LogicalNode rc = p.getInnerNode();
    UnaryNode lm = (UnaryNode)left;
    UnaryNode rm = (UnaryNode)right;
    lm.setInputSchema(lc.getOutputSchema());
    lm.setOutputSchema(lc.getOutputSchema());
    lm.setSubNode(lc);
    rm.setInputSchema(rc.getOutputSchema());
    rm.setOutputSchema(rc.getOutputSchema());
    rm.setSubNode(rc);
    p.setOuter(lm);
    p.setInner(rm);
    return p;
  }
  
  public static LogicalNode transformGroupbyTo2P(GroupbyNode gp) {
    Preconditions.checkNotNull(gp);
        
    try {
      GroupbyNode child = (GroupbyNode) gp.clone();
      gp.setSubNode(child);
      gp.setInputSchema(child.getOutputSchema());
      gp.setOutputSchema(child.getOutputSchema());
    
      Target [] targets = gp.getTargetList();
      for (int i = 0; i < gp.getTargetList().length; i++) {
        if (targets[i].getEvalTree().getType() == Type.FUNCTION) {
          Column tobe = child.getOutputSchema().getColumn(i);        
          FuncCallEval eval = (FuncCallEval) targets[i].getEvalTree();
          Collection<Column> tobeChanged = 
              EvalTreeUtil.findDistinctRefColumns(eval);
          EvalTreeUtil.changeColumnRef(eval, tobeChanged.iterator().next(), 
              tobe);
        }
      }
      
    } catch (CloneNotSupportedException e) {
      LOG.error(e);
    }
    
    return gp;
  }
  
  public static LogicalNode transformSortTo2P(SortNode sort) {
    Preconditions.checkNotNull(sort);
    
    try {
      SortNode child = (SortNode) sort.clone();
      sort.setSubNode(child);
      sort.setInputSchema(child.getOutputSchema());
      sort.setOutputSchema(child.getOutputSchema());
    } catch (CloneNotSupportedException e) {
      LOG.error(e);
    }
    return sort;
  }
  
  public static LogicalNode transformGroupbyTo2PWithStore(GroupbyNode gb, 
      String tableId) {
    GroupbyNode groupby = (GroupbyNode) transformGroupbyTo2P(gb);
    return insertStore(groupby, tableId);
  }
  
  public static LogicalNode transformSortTo2PWithStore(SortNode sort, 
      String tableId) {
    SortNode sort2p = (SortNode) transformSortTo2P(sort);
    return insertStore(sort2p, tableId);
  }
  
  private static LogicalNode insertStore(LogicalNode parent, 
      String tableId) {
    CreateTableNode store = new CreateTableNode(tableId);
    store.setLocal(true);
    insertNode(parent, store);
    
    return parent;
  }
  
  /**
   * Find the top node of the given plan
   * 
   * @param plan
   * @param type to find
   * @return a found logical node
   */
  public static LogicalNode findTopNode(LogicalNode plan, ExprType type) {
    Preconditions.checkNotNull(plan);
    Preconditions.checkNotNull(type);
    
    LogicalNodeFinder finder = new LogicalNodeFinder(type);
    plan.postOrder(finder);
    
    if (finder.getFoundNodes().size() == 0) {
      return null;
    }
    return finder.getFoundNodes().get(0);
  }
  
  /**
   * Find a parent node of a given-typed operator.
   * 
   * @param plan
   * @param type
   * @return the parent node of a found logical node
   */
  public static LogicalNode findTopParentNode(LogicalNode plan, ExprType type) {
    Preconditions.checkNotNull(plan);
    Preconditions.checkNotNull(type);
    
    ParentNodeFinder finder = new ParentNodeFinder(type);
    plan.postOrder(finder);
    
    if (finder.getFoundNodes().size() == 0) {
      return null;
    }
    return finder.getFoundNodes().get(0);
  }
  
  private static class LogicalNodeFinder implements LogicalNodeVisitor {
    private List<LogicalNode> list = new ArrayList<LogicalNode>();
    private ExprType tofind;

    public LogicalNodeFinder(ExprType type) {
      this.tofind = type;
    }

    @Override
    public void visit(LogicalNode node) {
      if (node.getType() == tofind) {
        list.add(node);
      }
    }

    public List<LogicalNode> getFoundNodes() {
      return list;
    }
  }
  
  private static class ParentNodeFinder implements LogicalNodeVisitor {
    private List<LogicalNode> list = new ArrayList<LogicalNode>();
    private ExprType tofind;

    public ParentNodeFinder(ExprType type) {
      this.tofind = type;
    }

    @Override
    public void visit(LogicalNode node) {
      if (node instanceof UnaryNode) {
        UnaryNode unary = (UnaryNode) node;
        if (unary.getSubNode().getType() == tofind) {
          list.add(node);
        }
      } else if (node instanceof BinaryNode){
        BinaryNode bin = (BinaryNode) node;
        if (bin.getOuterNode().getType() == tofind ||
            bin.getInnerNode().getType() == tofind) {
          list.add(node);
        }
      }
    }

    public List<LogicalNode> getFoundNodes() {
      return list;
    }
  }
  
  public static Set<Column> collectColumnRefs(LogicalNode node) {
    ColumnRefCollector collector = new ColumnRefCollector();
    node.postOrder(collector);
    return collector.getColumns();
  }
  
  private static class ColumnRefCollector implements LogicalNodeVisitor {
    private Set<Column> collected = Sets.newHashSet();
    
    public Set<Column> getColumns() {
      return this.collected;
    }

    @Override
    public void visit(LogicalNode node) {
      Set<Column> temp = null;
      switch (node.getType()) {
      case PROJECTION:
        ProjectionNode projNode = (ProjectionNode) node;

        if (!projNode.isAll()) {
          for (Target t : projNode.getTargetList()) {
            temp = EvalTreeUtil.findDistinctRefColumns(t.getEvalTree());
            if (!temp.isEmpty()) {
              collected.addAll(temp);
            }
          }
        }

        break;

      case SELECTION:
        SelectionNode selNode = (SelectionNode) node;
        temp = EvalTreeUtil.findDistinctRefColumns(selNode.getQual());
        if (!temp.isEmpty()) {
          collected.addAll(temp);
        }

        break;
        
      case GROUP_BY:
        GroupbyNode groupByNode = (GroupbyNode)node;
        collected.addAll(Lists.newArrayList(groupByNode.getGroupingColumns()));
        for (Target t : groupByNode.getTargetList()) {
          temp = EvalTreeUtil.findDistinctRefColumns(t.getEvalTree());
          if (!temp.isEmpty()) {
            collected.addAll(temp);
          }
        }
        if(groupByNode.hasHavingCondition()) {
          temp = EvalTreeUtil.findDistinctRefColumns(groupByNode.
              getHavingCondition());
          if (!temp.isEmpty()) {
            collected.addAll(temp);
          }
        }
        
        break;
        
      case SORT:
        SortNode sortNode = (SortNode) node;
        for (SortKey key : sortNode.getSortKeys()) {
          collected.add(key.getSortKey());
        }
        
        break;
        
      case JOIN:
        JoinNode joinNode = (JoinNode) node;
        if (joinNode.hasJoinQual()) {
          temp = EvalTreeUtil.findDistinctRefColumns(joinNode.getJoinQual());
          if (!temp.isEmpty()) {
            collected.addAll(temp);
          }
        }
        
        break;
        
      case SCAN:
        ScanNode scanNode = (ScanNode) node;
        if (scanNode.hasQual()) {
          temp = EvalTreeUtil.findDistinctRefColumns(scanNode.getQual());
          if (!temp.isEmpty()) {
            collected.addAll(temp);
          }
        }

        break;
        
      default:
      }
    }
  }
}
