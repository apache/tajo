/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.planner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.query.exception.InvalidQueryException;
import org.apache.tajo.storage.TupleComparator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class PlannerUtil {
  private static final Log LOG = LogFactory.getLog(PlannerUtil.class);

  public static String normalizeTableName(String tableName) {
    return tableName.toLowerCase();
  }

  public static boolean checkIfDDLPlan(LogicalNode node) {
    LogicalNode baseNode = node;
    if (node instanceof LogicalRootNode) {
      baseNode = ((LogicalRootNode) node).getChild();
    }

    return baseNode.getType() == NodeType.CREATE_TABLE || baseNode.getType() == NodeType.DROP_TABLE;
  }
  
  public static String [] getLineage(LogicalNode node) {
    LogicalNode [] scans =  PlannerUtil.findAllNodes(node, NodeType.SCAN);
    String [] tableNames = new String[scans.length];
    ScanNode scan;
    for (int i = 0; i < scans.length; i++) {
      scan = (ScanNode) scans[i];
      tableNames[i] = scan.getTableId();
    }
    return tableNames;
  }
  
  public static LogicalNode insertNode(LogicalNode parent, LogicalNode newNode) {
    Preconditions.checkArgument(parent instanceof UnaryNode);
    Preconditions.checkArgument(newNode instanceof UnaryNode);
    
    UnaryNode p = (UnaryNode) parent;
    LogicalNode c = p.getChild();
    UnaryNode m = (UnaryNode) newNode;
    m.setInSchema(c.getOutSchema());
    m.setOutSchema(c.getOutSchema());
    m.setChild(c);
    p.setChild(m);
    
    return p;
  }
  
  /**
   * Delete the logical node from a plan.
   *
   * @param parent this node must be a parent node of one node to be removed.
   * @param tobeRemoved this node must be a child node of the parent.
   */
  public static LogicalNode deleteNode(LogicalNode parent, LogicalNode tobeRemoved) {
    Preconditions.checkArgument(tobeRemoved instanceof UnaryNode,
        "ERROR: the logical node to be removed must be unary node.");

    UnaryNode child = (UnaryNode) tobeRemoved;
    LogicalNode grandChild = child.getChild();
    if (parent instanceof UnaryNode) {
      UnaryNode unaryParent = (UnaryNode) parent;

      Preconditions.checkArgument(unaryParent.getChild() == child,
          "ERROR: both logical node must be parent and child nodes");
      unaryParent.setChild(grandChild);

    } else if (parent instanceof BinaryNode) {
      BinaryNode binaryParent = (BinaryNode) parent;
      if (binaryParent.getLeftChild().equals(child)) {
        binaryParent.setLeftChild(grandChild);
      } else if (binaryParent.getRightChild().equals(child)) {
        binaryParent.setRightChild(grandChild);
      } else {
        throw new IllegalStateException("ERROR: both logical node must be parent and child nodes");
      }
    } else {
      throw new InvalidQueryException("Unexpected logical plan: " + parent);
    }    
    return child;
  }
  
  public static void replaceNode(LogicalNode plan, LogicalNode newNode, NodeType type) {
    LogicalNode parent = findTopParentNode(plan, type);
    Preconditions.checkArgument(parent instanceof UnaryNode);
    Preconditions.checkArgument(!(newNode instanceof BinaryNode));
    UnaryNode parentNode = (UnaryNode) parent;
    LogicalNode child = parentNode.getChild();
    if (child instanceof UnaryNode) {
      ((UnaryNode) newNode).setChild(((UnaryNode) child).getChild());
    }
    parentNode.setChild(newNode);
  }
  
  public static LogicalNode insertOuterNode(LogicalNode parent, LogicalNode outer) {
    Preconditions.checkArgument(parent instanceof BinaryNode);
    Preconditions.checkArgument(outer instanceof UnaryNode);
    
    BinaryNode p = (BinaryNode) parent;
    LogicalNode c = p.getLeftChild();
    UnaryNode m = (UnaryNode) outer;
    m.setInSchema(c.getOutSchema());
    m.setOutSchema(c.getOutSchema());
    m.setChild(c);
    p.setLeftChild(m);
    return p;
  }
  
  public static LogicalNode insertInnerNode(LogicalNode parent, LogicalNode inner) {
    Preconditions.checkArgument(parent instanceof BinaryNode);
    Preconditions.checkArgument(inner instanceof UnaryNode);
    
    BinaryNode p = (BinaryNode) parent;
    LogicalNode c = p.getRightChild();
    UnaryNode m = (UnaryNode) inner;
    m.setInSchema(c.getOutSchema());
    m.setOutSchema(c.getOutSchema());
    m.setChild(c);
    p.setRightChild(m);
    return p;
  }
  
  public static LogicalNode insertNode(LogicalNode parent, 
      LogicalNode left, LogicalNode right) {
    Preconditions.checkArgument(parent instanceof BinaryNode);
    Preconditions.checkArgument(left instanceof UnaryNode);
    Preconditions.checkArgument(right instanceof UnaryNode);
    
    BinaryNode p = (BinaryNode)parent;
    LogicalNode lc = p.getLeftChild();
    LogicalNode rc = p.getRightChild();
    UnaryNode lm = (UnaryNode)left;
    UnaryNode rm = (UnaryNode)right;
    lm.setInSchema(lc.getOutSchema());
    lm.setOutSchema(lc.getOutSchema());
    lm.setChild(lc);
    rm.setInSchema(rc.getOutSchema());
    rm.setOutSchema(rc.getOutSchema());
    rm.setChild(rc);
    p.setLeftChild(lm);
    p.setRightChild(rm);
    return p;
  }
  
  public static LogicalNode transformGroupbyTo2P(GroupbyNode gp) {
    Preconditions.checkNotNull(gp);
        
    try {
      // cloning groupby node
      GroupbyNode child = (GroupbyNode) gp.clone();

      List<Target> newChildTargets = Lists.newArrayList();
      Target[] secondTargets = gp.getTargets();
      Target[] firstTargets = child.getTargets();

      Target second;
      Target first;
      int targetId =  0;
      for (int i = 0; i < firstTargets.length; i++) {
        second = secondTargets[i];
        first = firstTargets[i];

        List<AggFuncCallEval> secondFuncs = EvalTreeUtil
            .findDistinctAggFunction(second.getEvalTree());
        List<AggFuncCallEval> firstFuncs = EvalTreeUtil.findDistinctAggFunction(first.getEvalTree());

        if (firstFuncs.size() == 0) {
          newChildTargets.add(first);
          targetId++;
        } else {
          for (AggFuncCallEval func : firstFuncs) {
            func.setFirstPhase();
            Target newTarget = new Target(func);
            String targetName = "column_" + (targetId++);
            newTarget.setAlias(targetName);

            AggFuncCallEval secondFunc = null;
            for (AggFuncCallEval sf : secondFuncs) {
              if (func.equals(sf)) {
                secondFunc = sf;
                break;
              }
            }
            if (func.getValueType().length > 1) { // hack for partial result
              secondFunc.setArgs(new EvalNode[] {new FieldEval(new Column(targetName, Type.ARRAY))});
            } else {
              secondFunc.setArgs(new EvalNode [] {new FieldEval(
                  new Column(targetName, newTarget.getEvalTree().getValueType()[0]))});
            }
            newChildTargets.add(newTarget);
          }
        }
      }

      Target[] targetArray = newChildTargets.toArray(new Target[newChildTargets.size()]);
      child.setTargets(targetArray);
      child.setOutSchema(PlannerUtil.targetToSchema(targetArray));
      // set the groupby chaining
      gp.setChild(child);
      gp.setInSchema(child.getOutSchema());
    } catch (CloneNotSupportedException e) {
      LOG.error(e);
    }
    
    return gp;
  }
  
  public static LogicalNode transformSortTo2P(SortNode sort) {
    Preconditions.checkNotNull(sort);
    
    try {
      SortNode child = (SortNode) sort.clone();
      sort.setChild(child);
      sort.setInSchema(child.getOutSchema());
      sort.setOutSchema(child.getOutSchema());
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
  public static LogicalNode findTopNode(LogicalNode node, NodeType type) {
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
  public static LogicalNode [] findAllNodes(LogicalNode node, NodeType type) {
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
  public static LogicalNode findTopParentNode(LogicalNode node, NodeType type) {
    Preconditions.checkNotNull(node);
    Preconditions.checkNotNull(type);
    
    ParentNodeFinder finder = new ParentNodeFinder(type);
    node.postOrder(finder);
    
    if (finder.getFoundNodes().size() == 0) {
      return null;
    }
    return finder.getFoundNodes().get(0);
  }

  public static boolean canBeEvaluated(EvalNode eval, LogicalNode node) {
    Set<Column> columnRefs = EvalTreeUtil.findDistinctRefColumns(eval);

    if (node.getType() == NodeType.JOIN) {
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

      String [] outer = getLineage(joinNode.getLeftChild());
      String [] inner = getLineage(joinNode.getRightChild());

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

      return i.contains(it.next()) && o.contains(it.next());

    } else {
      if (node instanceof ScanNode) {
        ScanNode scan = (ScanNode) node;

        for (Column col : columnRefs) {
          if (scan.getTableId().equals(col.getTableName())) {
            Column found = node.getInSchema().getColumnByName(col.getColumnName());
            if (found == null) {
              return false;
            }
          } else {
            return false;
          }
        }
      } else {
        for (Column col : columnRefs) {
          if (!node.getInSchema().contains(col.getQualifiedName())) {
            return false;
          }
        }
      }

      return true;
    }
  }

  private static class LogicalNodeFinder implements LogicalNodeVisitor {
    private List<LogicalNode> list = new ArrayList<LogicalNode>();
    private final NodeType[] tofind;
    private boolean topmost = false;
    private boolean finished = false;

    public LogicalNodeFinder(NodeType...type) {
      this.tofind = type;
    }

    public LogicalNodeFinder(NodeType[] type, boolean topmost) {
      this(type);
      this.topmost = topmost;
    }

    @Override
    public void visit(LogicalNode node) {
      if (!finished) {
        for (NodeType type : tofind) {
          if (node.getType() == type) {
            list.add(node);
          }
          if (topmost && list.size() > 0) {
            finished = true;
          }
        }
      }
    }

    public List<LogicalNode> getFoundNodes() {
      return list;
    }
  }
  
  private static class ParentNodeFinder implements LogicalNodeVisitor {
    private List<LogicalNode> list = new ArrayList<LogicalNode>();
    private NodeType tofind;

    public ParentNodeFinder(NodeType type) {
      this.tofind = type;
    }

    @Override
    public void visit(LogicalNode node) {
      if (node instanceof UnaryNode) {
        UnaryNode unary = (UnaryNode) node;
        if (unary.getChild().getType() == tofind) {
          list.add(node);
        }
      } else if (node instanceof BinaryNode){
        BinaryNode bin = (BinaryNode) node;
        if (bin.getLeftChild().getType() == tofind ||
            bin.getRightChild().getType() == tofind) {
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

  /**
   * fill targets with FieldEvals from a given schema
   *
   * @param schema to be transformed to targets
   * @param targets to be filled
   */
  public static void schemaToTargets(Schema schema, Target [] targets) {
    FieldEval eval;
    for (int i = 0; i < schema.getColumnNum(); i++) {
      eval = new FieldEval(schema.getColumn(i));
      targets[i] = new Target(eval);
    }
  }

  public static Target[] schemaToTargets(Schema schema) {
    Target[] targets = new Target[schema.getColumnNum()];

    FieldEval eval;
    for (int i = 0; i < schema.getColumnNum(); i++) {
      eval = new FieldEval(schema.getColumn(i));
      targets[i] = new Target(eval);
    }
    return targets;
  }

  public static SortSpec[] schemaToSortSpecs(Schema schema) {
    SortSpec[] specs = new SortSpec[schema.getColumnNum()];

    for (int i = 0; i < schema.getColumnNum(); i++) {
      specs[i] = new SortSpec(schema.getColumn(i), true, false);
    }

    return specs;
  }

  public static Schema sortSpecsToSchema(SortSpec[] sortSpecs) {
    Schema schema = new Schema();
    for (SortSpec spec : sortSpecs) {
      schema.addColumn(spec.getSortKey());
    }

    return schema;
  }

  /**
   * is it join qual or not?
   *
   * @param qual  The condition to be checked
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
    SortSpec[] outerSortSpec = new SortSpec[joinKeyPairs.size()];
    SortSpec[] innerSortSpec = new SortSpec[joinKeyPairs.size()];

    for (int i = 0; i < joinKeyPairs.size(); i++) {
      outerSortSpec[i] = new SortSpec(joinKeyPairs.get(i)[0]);
      innerSortSpec[i] = new SortSpec(joinKeyPairs.get(i)[1]);
    }

    return new SortSpec[][] {outerSortSpec, innerSortSpec};
  }

  public static TupleComparator[] getComparatorsFromJoinQual(EvalNode joinQual, Schema outer, Schema inner) {
    SortSpec[][] sortSpecs = getSortKeysFromJoinQual(joinQual, outer, inner);
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
      if (EvalTreeUtil.isJoinQual(node)) {
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

  public static Schema targetToSchema(Target[] targets) {
    Schema schema = new Schema();
    for(Target t : targets) {
      DataType type;
      if (t.getEvalTree().getValueType().length > 1) {
        type = CatalogUtil.newDataTypeWithoutLen(Type.ARRAY);
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

  /**
   * It removes all table names from FieldEvals in targets
   *
   * @param sourceTargets The targets to be stripped
   * @return The stripped targets
   */
  public static Target [] stripTarget(Target [] sourceTargets) {
    Target [] copy = new Target[sourceTargets.length];
    for(int i = 0; i < sourceTargets.length; i++) {
      try {
        copy[i] = (Target) sourceTargets[i].clone();
      } catch (CloneNotSupportedException e) {
        throw new InternalError(e.getMessage());
      }
      if (copy[i].getEvalTree().getType() == EvalType.FIELD) {
        FieldEval fieldEval = (FieldEval) copy[i].getEvalTree();
        if (fieldEval.getColumnRef().isQualified()) {
          fieldEval.getColumnRef().setName(fieldEval.getColumnName());
        }
      }
    }

    return copy;
  }
}
