package nta.engine.planner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos;
import nta.engine.Context;
import nta.engine.exec.eval.*;
import nta.engine.parser.QueryBlock.SortSpec;
import nta.engine.parser.QueryBlock.Target;
import nta.engine.planner.logical.*;
import nta.engine.planner.physical.TupleComparator;
import nta.engine.query.exception.InvalidQueryException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 * @author Hyunsik Choi
 */
public class PlannerUtil {
  private static final Log LOG = LogFactory.getLog(PlannerUtil.class);
  
  public static String [] getLineage(LogicalNode node) {
    LogicalNode [] scans =  PlannerUtil.findAllNodes(node, ExprType.SCAN);
    String [] tableNames = new String[scans.length];
    ScanNode scan;
    for (int i = 0; i < scans.length; i++) {
      scan = (ScanNode) scans[i];
      /*if (scan.hasAlias()) {
        tableNames[i] = scan.getAlias();
      } else {*/
        tableNames[i] = scan.getTableId();
      //}
    }
    return tableNames;
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
   * @return input parent node
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
      // cloning groupby node
      GroupbyNode child = (GroupbyNode) gp.clone();

      List<Target> newChildTargets = Lists.newArrayList();
      Target [] secondTargets = gp.getTargets();
      Target [] firstTargets = child.getTargets();

      Target second;
      Target first;
      int firstTargetId = 0;
      for (int i = 0; i < firstTargets.length; i++) {
        second = secondTargets[i];
        first = firstTargets[i];

        List<AggFuncCallEval> secondFuncs = EvalTreeUtil.findDistinctAggFunction(second.getEvalTree());
        List<AggFuncCallEval> firstFuncs = EvalTreeUtil.findDistinctAggFunction(first.getEvalTree());

        if (firstFuncs.size() == 0) {
          newChildTargets.add(first);
          firstTargetId++;
        } else {
          for (AggFuncCallEval func : firstFuncs) {
            func.setFirstPhase();
            Target newTarget = new Target(func, firstTargetId++);
            newTarget.setAlias("column_"+ firstTargetId);


            AggFuncCallEval secondFunc = null;
            for (AggFuncCallEval sf : secondFuncs) {
              if (func.equals(sf)) {
                secondFunc = sf;
                break;
              }
            }
            if (func.getValueType().length > 1) { // hack for partial result
              secondFunc.setArgs(new EvalNode [] {new FieldEval(
                  new Column("column_"+firstTargetId, CatalogProtos.DataType.ARRAY))});
            } else {
              secondFunc.setArgs(new EvalNode [] {new FieldEval(
                  new Column("column_"+firstTargetId, newTarget.getEvalTree().getValueType()[0]))});
            }
            newChildTargets.add(newTarget);
          }
        }
      }

      Target [] targetArray = newChildTargets.toArray(new Target[newChildTargets.size()]);
      child.setTargetList(targetArray);
      child.setOutputSchema(PlannerUtil.targetToSchema(targetArray));
      // set the groupby chaining
      gp.setSubNode(child);
      gp.setInputSchema(child.getOutputSchema());
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
    StoreTableNode store = new StoreTableNode(tableId);
    store.setLocal(true);
    insertNode(parent, store);
    
    return parent;
  }
  
  /**
   * Find the top logical node matched to type from the given node
   * 
   * @param node start node
   * @param type to find
   * @return a found logical node
   */
  public static LogicalNode findTopNode(LogicalNode node, ExprType type) {
    Preconditions.checkNotNull(node);
    Preconditions.checkNotNull(type);
    
    LogicalNodeFinder finder = new LogicalNodeFinder(type);
    node.postOrder(finder);
    
    if (finder.getFoundNodes().size() == 0) {
      return null;
    }
    return finder.getFoundNodes().get(0);
  }

  /**
   * Find the all logical node matched to type from the given node
   *
   * @param node start node
   * @param type to find
   * @return a found logical node
   */
  public static LogicalNode [] findAllNodes(LogicalNode node, ExprType type) {
    Preconditions.checkNotNull(node);
    Preconditions.checkNotNull(type);

    LogicalNodeFinder finder = new LogicalNodeFinder(type);
    node.postOrder(finder);

    if (finder.getFoundNodes().size() == 0) {
      return new LogicalNode[] {};
    }
    List<LogicalNode> founds = finder.getFoundNodes();
    return founds.toArray(new LogicalNode[founds.size()]);
  }
  
  /**
   * Find a parent node of a given-typed operator.
   * 
   * @param node start node
   * @param type to find
   * @return the parent node of a found logical node
   */
  public static LogicalNode findTopParentNode(LogicalNode node, ExprType type) {
    Preconditions.checkNotNull(node);
    Preconditions.checkNotNull(type);
    
    ParentNodeFinder finder = new ParentNodeFinder(type);
    node.postOrder(finder);
    
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
      Set<Column> temp;
      switch (node.getType()) {
      case PROJECTION:
        ProjectionNode projNode = (ProjectionNode) node;

        for (Target t : projNode.getTargets()) {
          temp = EvalTreeUtil.findDistinctRefColumns(t.getEvalTree());
          if (!temp.isEmpty()) {
            collected.addAll(temp);
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
        for (Target t : groupByNode.getTargets()) {
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
        for (SortSpec key : sortNode.getSortKeys()) {
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

  public static Target [] schemaToTargets(Schema schema) {
    Target [] targets = new Target[schema.getColumnNum()];

    FieldEval eval;
    for (int i = 0; i < schema.getColumnNum(); i++) {
      eval = new FieldEval(schema.getColumn(i));
      targets[i] = new Target(eval, i);
    }
    return targets;
  }

  public static SortSpec [] schemaToSortSpecs(Schema schema) {
    SortSpec [] specs = new SortSpec[schema.getColumnNum()];

    for (int i = 0; i < schema.getColumnNum(); i++) {
      specs[i] = new SortSpec(schema.getColumn(i), true, false);
    }

    return specs;
  }

  public static Schema sortSpecsToSchema(SortSpec [] sortSpecs) {
    Schema schema = new Schema();
    for (SortSpec spec : sortSpecs) {
      schema.addColumn(spec.getSortKey());
    }

    return schema;
  }

  /**
   * is it join qual or not?
   * TODO - this method does not support the self join (NTA-740)
   * @param qual
   * @return true if two operands refers to columns and the operator is comparison,
   */
  public static boolean isJoinQual(EvalNode qual) {
    if (EvalTreeUtil.isComparisonOperator(qual)) {
      List<Column> left = EvalTreeUtil.findAllColumnRefs(qual.getLeftExpr());
      List<Column> right = EvalTreeUtil.findAllColumnRefs(qual.getRightExpr());

      if (left.size() == 1 && right.size() == 1 &&
          !left.get(0).getTableName().equals(right.get(0).getTableName()))
        return true;
    }

    return false;
  }

  public static SortSpec[][] getSortKeysFromJoinQual(EvalNode joinQual, Schema outer, Schema inner) {
    List<Column []> joinKeyPairs = getJoinKeyPairs(joinQual, outer, inner);
    SortSpec [] outerSortSpec = new SortSpec[joinKeyPairs.size()];
    SortSpec [] innerSortSpec = new SortSpec[joinKeyPairs.size()];

    for (int i = 0; i < joinKeyPairs.size(); i++) {
      outerSortSpec[i] = new SortSpec(joinKeyPairs.get(i)[0]);
      innerSortSpec[i] = new SortSpec(joinKeyPairs.get(i)[1]);
    }

    return new SortSpec[][] {outerSortSpec, innerSortSpec};
  }

  public static TupleComparator [] getComparatorsFromJoinQual(EvalNode joinQual, Schema outer, Schema inner) {
    SortSpec [][] sortSpecs = getSortKeysFromJoinQual(joinQual, outer, inner);
    TupleComparator [] comparators = new TupleComparator[2];
    comparators[0] = new TupleComparator(outer, sortSpecs[0]);
    comparators[1] = new TupleComparator(inner, sortSpecs[1]);
    return comparators;
  }

  public static List<Column []> getJoinKeyPairs(EvalNode joinQual, Schema outer, Schema inner) {
    JoinKeyPairFinder finder = new JoinKeyPairFinder(outer, inner);
    joinQual.preOrder(finder);
    return finder.getPairs();
  }

  public static class JoinKeyPairFinder implements EvalNodeVisitor {
    private final List<Column []> pairs = Lists.newArrayList();
    private Schema [] schemas = new Schema[2];

    public JoinKeyPairFinder(Schema outer, Schema inner) {
      schemas[0] = outer;
      schemas[1] = inner;
    }

    @Override
    public void visit(EvalNode node) {
      if (EvalTreeUtil.isComparisonOperator(node)) {
        Column [] pair = new Column[2];

        for (int i = 0; i <= 1; i++) { // access left, right sub expression
          Column column = EvalTreeUtil.findAllColumnRefs(node.getExpr(i)).get(0);
          for (int j = 0; j < schemas.length; j++) {
          // check whether the column is for either outer or inner
          // 0 is outer, and 1 is inner
            if (schemas[j].contains(column.getQualifiedName())) {
              pair[j] = column;
            }
          }
        }

        if (pair[0] == null || pair[1] == null) {
          throw new IllegalStateException("Wrong join key: " + node);
        }
        pairs.add(pair);
      }
    }

    public List<Column []> getPairs() {
      return this.pairs;
    }
  }

  public static Schema targetToSchema(Context ctx, Target[] targets) {
    Schema schema = new Schema();
    for(Target t : targets) {
      CatalogProtos.DataType type = t.getEvalTree().getValueType()[0];
      String name;
      if (t.hasAlias()) {
        name = t.getAlias();
      } else if (t.getEvalTree().getName().equals("?")) {
        name = ctx.getUnnamedColumn();
      } else {
        name = t.getEvalTree().getName();
      }
      schema.addColumn(name, type);
    }

    return schema;
  }

  public static Schema targetToSchema(Target[] targets) {
    Schema schema = new Schema();
    for(Target t : targets) {
      CatalogProtos.DataType type;
      if (t.getEvalTree().getValueType().length > 1) {
        type = CatalogProtos.DataType.ARRAY;
      } else {
        type = t.getEvalTree().getValueType()[0];
      }
      String name;
      if (t.hasAlias()) {
        name = t.getAlias();
      } else {
        name = t.getEvalTree().getName();
      }
      schema.addColumn(name, type);
    }

    return schema;
  }

  public static EvalNode [] columnsToEvals(Column [] columns) {
    EvalNode [] exprs = new EvalNode[columns.length];
    for (int i = 0; i < columns.length; i++) {
      exprs[i] = new FieldEval(columns[i]);
    }
    return exprs;
  }
}
