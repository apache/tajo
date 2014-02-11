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
import org.apache.tajo.algebra.CountRowsFunctionExpr;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.GeneralSetFunctionExpr;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.exception.InvalidQueryException;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.utils.SchemaUtil;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.util.TUtil;

import java.util.*;

public class PlannerUtil {
  public static String normalizeTableName(String tableName) {
    return tableName.toLowerCase();
  }

  public static boolean checkIfDDLPlan(LogicalNode node) {
    LogicalNode baseNode = node;
    if (node instanceof LogicalRootNode) {
      baseNode = ((LogicalRootNode) node).getChild();
    }

    return (baseNode.getType() == NodeType.CREATE_TABLE && !((CreateTableNode)baseNode).hasSubQuery()) ||
        baseNode.getType() == NodeType.DROP_TABLE;
  }

  /**
   * Get all scan nodes from a logical operator tree.
   *
   * @param node a start node
   * @return an array of relation names
   */
  public static String [] getRelationLineage(LogicalNode node) {
    LogicalNode [] scans =  PlannerUtil.findAllNodes(node, NodeType.SCAN);
    String [] tableNames = new String[scans.length];
    ScanNode scan;
    for (int i = 0; i < scans.length; i++) {
      scan = (ScanNode) scans[i];
      tableNames[i] = scan.getCanonicalName();
    }
    return tableNames;
  }

  /**
   * Get all scan nodes from a logical operator tree within a query block
   *
   * @param node a start node
   * @return an array of relation names
   */
  public static Collection<String> getRelationLineageWithinQueryBlock(LogicalPlan plan, LogicalNode node)
      throws PlanningException {
    RelationFinderVisitor visitor = new RelationFinderVisitor();
    visitor.visit(null, plan, null, node, new Stack<LogicalNode>());
    return visitor.getFoundRelations();
  }

  public static class RelationFinderVisitor extends BasicLogicalPlanVisitor<Object, LogicalNode> {
    private Set<String> foundRelNameSet = Sets.newHashSet();

    public Set<String> getFoundRelations() {
      return foundRelNameSet;
    }

    @Override
    public LogicalNode visit(Object context, LogicalPlan plan, @Nullable LogicalPlan.QueryBlock block, LogicalNode node,
                             Stack<LogicalNode> stack) throws PlanningException {
      if (node.getType() != NodeType.TABLE_SUBQUERY) {
        super.visit(context, plan, block, node, stack);
      }

      if (node instanceof RelationNode) {
        foundRelNameSet.add(((RelationNode) node).getCanonicalName());
      }

      return node;
    }
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
      if (binaryParent.getLeftChild().deepEquals(child)) {
        binaryParent.setLeftChild(grandChild);
      } else if (binaryParent.getRightChild().deepEquals(child)) {
        binaryParent.setRightChild(grandChild);
      } else {
        throw new IllegalStateException("ERROR: both logical node must be parent and child nodes");
      }
    } else {
      throw new InvalidQueryException("Unexpected logical plan: " + parent);
    }    
    return child;
  }

  public static void replaceNode(LogicalPlan plan, LogicalNode startNode, LogicalNode oldNode, LogicalNode newNode) {
    LogicalNodeReplaceVisitor replacer = new LogicalNodeReplaceVisitor(oldNode, newNode);
    try {
      replacer.visit(new ReplacerContext(), plan, null, startNode, new Stack<LogicalNode>());
    } catch (PlanningException e) {
      e.printStackTrace();
    }
  }

  static class ReplacerContext {
    boolean updateSchemaFlag = false;
  }

  public static class LogicalNodeReplaceVisitor extends BasicLogicalPlanVisitor<ReplacerContext, LogicalNode> {
    private LogicalNode target;
    private LogicalNode tobeReplaced;

    public LogicalNodeReplaceVisitor(LogicalNode target, LogicalNode tobeReplaced) {
      this.target = target;
      this.tobeReplaced = tobeReplaced;
    }

    /**
     * If this node can have child, it returns TRUE. Otherwise, it returns FALSE.
     */
    private static boolean checkIfVisitable(LogicalNode node) {
      return node instanceof UnaryNode || node instanceof BinaryNode;
    }

    @Override
    public LogicalNode visit(ReplacerContext context, LogicalPlan plan, @Nullable LogicalPlan.QueryBlock block,
                             LogicalNode node, Stack<LogicalNode> stack) throws PlanningException {
      LogicalNode left = null;
      LogicalNode right = null;

      if (node instanceof UnaryNode) {
        UnaryNode unaryNode = (UnaryNode) node;
        if (unaryNode.getChild().deepEquals(target)) {
          unaryNode.setChild(tobeReplaced);
          left = tobeReplaced;
          context.updateSchemaFlag = true;
        } else if (checkIfVisitable(unaryNode.getChild())) {
          left = visit(context, plan, null, unaryNode.getChild(), stack);
        }
      } else if (node instanceof BinaryNode) {
        BinaryNode binaryNode = (BinaryNode) node;
        if (binaryNode.getLeftChild().deepEquals(target)) {
          binaryNode.setLeftChild(tobeReplaced);
          left = tobeReplaced;
          context.updateSchemaFlag = true;
        } else if (checkIfVisitable(binaryNode.getLeftChild())) {
          left = visit(context, plan, null, binaryNode.getLeftChild(), stack);
        } else {
          left = binaryNode.getLeftChild();
        }

        if (binaryNode.getRightChild().deepEquals(target)) {
          binaryNode.setRightChild(tobeReplaced);
          right = tobeReplaced;
          context.updateSchemaFlag = true;
        } else if (checkIfVisitable(binaryNode.getRightChild())) {
          right = visit(context, plan, null, binaryNode.getRightChild(), stack);
        } else {
          right = binaryNode.getRightChild();
        }
      }

      // update schemas of nodes except for leaf node (i.e., RelationNode)
      if (context.updateSchemaFlag) {
        if (node instanceof Projectable) {
          if (node instanceof BinaryNode) {
            node.setInSchema(SchemaUtil.merge(left.getOutSchema(), right.getOutSchema()));
          } else {
            node.setInSchema(left.getOutSchema());
          }
          context.updateSchemaFlag = false;
        } else {
          node.setInSchema(left.getOutSchema());
          node.setOutSchema(left.getOutSchema());
        }
      }
      return node;
    }

    @Override
    public LogicalNode visitScan(ReplacerContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, ScanNode node,
                                 Stack<LogicalNode> stack) throws PlanningException {
      return node;
    }

    @Override
    public LogicalNode visitPartitionedTableScan(ReplacerContext context, LogicalPlan plan, LogicalPlan.
        QueryBlock block,PartitionedTableScanNode node, Stack<LogicalNode> stack)

        throws PlanningException {
      return node;
    }
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

  /**
   * Find the top logical node matched to type from the given node
   * 
   * @param node start node
   * @param type to find
   * @return a found logical node
   */
  public static <T extends LogicalNode> T findTopNode(LogicalNode node, NodeType type) {
    Preconditions.checkNotNull(node);
    Preconditions.checkNotNull(type);
    
    LogicalNodeFinder finder = new LogicalNodeFinder(type);
    node.preOrder(finder);
    
    if (finder.getFoundNodes().size() == 0) {
      return null;
    }
    return (T) finder.getFoundNodes().get(0);
  }

  /**
   * Find the most bottom logical node matched to type from the given node
   *
   * @param node start node
   * @param type to find
   * @return a found logical node
   */
  public static <T extends LogicalNode> T findMostBottomNode(LogicalNode node, NodeType type) {
    Preconditions.checkNotNull(node);
    Preconditions.checkNotNull(type);

    LogicalNodeFinder finder = new LogicalNodeFinder(type);
    node.preOrder(finder);

    if (finder.getFoundNodes().size() == 0) {
      return null;
    }
    return (T) finder.getFoundNodes().get(finder.getFoundNodes().size() - 1);
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
  public static <T extends LogicalNode> T findTopParentNode(LogicalNode node, NodeType type) {
    Preconditions.checkNotNull(node);
    Preconditions.checkNotNull(type);
    
    ParentNodeFinder finder = new ParentNodeFinder(type);
    node.postOrder(finder);
    
    if (finder.getFoundNodes().size() == 0) {
      return null;
    }
    return (T) finder.getFoundNodes().get(0);
  }

  public static boolean canBeEvaluated(EvalNode eval, LogicalNode node) {
    Set<Column> columnRefs = EvalTreeUtil.findDistinctRefColumns(eval);

    if (node.getType() == NodeType.JOIN) {
      JoinNode joinNode = (JoinNode) node;
      Set<String> tableIds = Sets.newHashSet();
      // getting distinct table references
      for (Column col : columnRefs) {
        if (!tableIds.contains(col.getQualifier())) {
          tableIds.add(col.getQualifier());
        }
      }

      // if the references only indicate two relation, the condition can be
      // pushed into a join operator.
      if (tableIds.size() != 2) {
        return false;
      }

      String [] outer = getRelationLineage(joinNode.getLeftChild());
      String [] inner = getRelationLineage(joinNode.getRightChild());

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

    } else if (node instanceof ScanNode) {

      RelationNode scan = (RelationNode) node;

      for (Column col : columnRefs) {
        if (scan.getCanonicalName().equals(col.getQualifier())) {
          Column found = scan.getTableSchema().getColumnByName(col.getColumnName());
          if (found == null) {
            return false;
          }
        } else {
          return false;
        }
      }

    } else if (node instanceof TableSubQueryNode) {
      TableSubQueryNode subQueryNode = (TableSubQueryNode) node;
      for (Column col : columnRefs) {
        if (subQueryNode.getCanonicalName().equals(col.getQualifier())) {
          Column found = node.getOutSchema().getColumnByName(col.getColumnName());
          if (found == null) {
            return false;
          }
        } else {
          return false;
        }
      }

    } else {

      for (Column col : columnRefs) {
        if (!node.getInSchema().containsByQualifiedName(col.getQualifiedName())) {
          return false;
        }
      }
    }

    return true;
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

  public static Target[] schemaToTargetsWithGeneratedFields(Schema schema) {
    List<Target> targets = TUtil.newList();

    FieldEval eval;
    Column column;
    for (int i = 0; i < schema.getColumnNum(); i++) {
      column = schema.getColumn(i);
      if (column.getColumnName().charAt(0) != LogicalPlan.NONAMED_COLUMN_PREFIX) {
        eval = new FieldEval(schema.getColumn(i));
        targets.add(new Target(eval));
      }
    }
    return targets.toArray(new Target[targets.size()]);
  }

  public static SortSpec[] schemaToSortSpecs(Schema schema) {
    return schemaToSortSpecs(schema.toArray());
  }

  public static SortSpec[] schemaToSortSpecs(Column [] columns) {
    SortSpec[] specs = new SortSpec[columns.length];

    for (int i = 0; i < columns.length; i++) {
      specs[i] = new SortSpec(columns[i], true, false);
    }

    return specs;
  }

  public static SortSpec [] columnsToSortSpec(Collection<Column> columns) {
    SortSpec[] specs = new SortSpec[columns.size()];
    int i = 0;
    for (Column column : columns) {
      specs[i++] = new SortSpec(column, true, false);
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
    if (AlgebraicUtil.isComparisonOperator(qual)) {
      List<Column> left = EvalTreeUtil.findAllColumnRefs(qual.getLeftExpr());
      List<Column> right = EvalTreeUtil.findAllColumnRefs(qual.getRightExpr());

      if (left.size() == 1 && right.size() == 1 &&
          !left.get(0).getQualifier().equals(right.get(0).getQualifier()))
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

  public static TupleComparator[] getComparatorsFromJoinQual(EvalNode joinQual, Schema leftSchema, Schema rightSchema) {
    SortSpec[][] sortSpecs = getSortKeysFromJoinQual(joinQual, leftSchema, rightSchema);
    TupleComparator [] comparators = new TupleComparator[2];
    comparators[0] = new TupleComparator(leftSchema, sortSpecs[0]);
    comparators[1] = new TupleComparator(rightSchema, sortSpecs[1]);
    return comparators;
  }

  /**
   * @return the first array contains left table's columns, and the second array contains right table's columns.
   */
  public static Column [][] joinJoinKeyForEachTable(EvalNode joinQual, Schema leftSchema, Schema rightSchema) {
    List<Column []> joinKeys = getJoinKeyPairs(joinQual, leftSchema, rightSchema);
    Column [] leftColumns = new Column[joinKeys.size()];
    Column [] rightColumns = new Column[joinKeys.size()];
    for (int i = 0; i < joinKeys.size(); i++) {
      leftColumns[i] = joinKeys.get(i)[0];
      rightColumns[i] = joinKeys.get(i)[1];
    }

    return new Column[][] {leftColumns, rightColumns};
  }

  public static List<Column []> getJoinKeyPairs(EvalNode joinQual, Schema leftSchema, Schema rightSchema) {
    JoinKeyPairFinder finder = new JoinKeyPairFinder(leftSchema, rightSchema);
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
            if (schemas[j].containsByQualifiedName(column.getQualifiedName())) {
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

  public static Schema targetToSchema(Collection<Target> targets) {
    return targetToSchema(targets.toArray(new Target[targets.size()]));
  }

  public static Schema targetToSchema(Target[] targets) {
    Schema schema = new Schema();
    for(Target t : targets) {
      DataType type = t.getEvalTree().getValueType();
      String name;
      if (t.hasAlias()) {
        name = t.getAlias();
      } else {
        name = t.getEvalTree().getName();
      }
      if (!schema.containsByQualifiedName(name)) {
        schema.addColumn(name, type);
      }
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
        FieldEval fieldEval = copy[i].getEvalTree();
        if (fieldEval.getColumnRef().hasQualifier()) {
          fieldEval.getColumnRef().setName(fieldEval.getColumnName());
        }
      }
    }

    return copy;
  }

  public static <T extends LogicalNode> T clone(LogicalPlan plan, LogicalNode node) {
    try {
      T copy = (T) node.clone();
      if (plan == null) {
        copy.setPID(-1);
      } else {
        copy.setPID(plan.newPID());
      }
      return copy;
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  public static boolean isCommutativeJoin(JoinType joinType) {
    return joinType == JoinType.INNER;
  }

  public static Schema rewriteColumnPartitionedTableSchema(
                               PartitionMethodDesc partitionDesc,
                               Schema columnPartitionSchema,
                               Schema sourceSchema,
                               String qualifier) {
    Schema schema = new Schema();
    for (Column column : sourceSchema.toArray()) {
      if (columnPartitionSchema.getColumnByName(column.getColumnName()) == null) {
        schema.addColumn(column);
      }
    }
    return schema;
  }

  public static boolean existsAggregationFunction(Expr expr) throws PlanningException {
    AggregationFunctionFinder finder = new AggregationFunctionFinder();
    AggFunctionFoundResult result = new AggFunctionFoundResult();
    finder.visit(result, new Stack<Expr>(), expr);
    return result.generalSetFunction;
  }

  public static boolean existsDistinctAggregationFunction(Expr expr) throws PlanningException {
    AggregationFunctionFinder finder = new AggregationFunctionFinder();
    AggFunctionFoundResult result = new AggFunctionFoundResult();
    finder.visit(result, new Stack<Expr>(), expr);
    return result.distinctSetFunction;
  }

  static class AggFunctionFoundResult {
    boolean generalSetFunction;
    boolean distinctSetFunction;
  }
  static class AggregationFunctionFinder extends SimpleAlgebraVisitor<AggFunctionFoundResult, Object> {
    @Override
    public Object visitCountRowsFunction(AggFunctionFoundResult ctx, Stack<Expr> stack, CountRowsFunctionExpr expr)
        throws PlanningException {
      ctx.generalSetFunction = true;
      return super.visitCountRowsFunction(ctx, stack, expr);
    }

    @Override
    public Object visitGeneralSetFunction(AggFunctionFoundResult ctx, Stack<Expr> stack, GeneralSetFunctionExpr expr)
        throws PlanningException {
      ctx.generalSetFunction = true;
      ctx.distinctSetFunction = expr.isDistinct();
      return super.visitGeneralSetFunction(ctx, stack, expr);
    }
  }

  public static Collection<String> toQualifiedFieldNames(Collection<String> fieldNames, String qualifier) {
    List<String> names = TUtil.newList();
    for (String n : fieldNames) {
      String [] parts = n.split("\\.");
      if (parts.length == 1) {
        names.add(qualifier + "." + parts[0]);
      } else {
        names.add(qualifier + "." + parts[1]);
      }
    }
    return names;
  }

  public static SortSpec [] convertSortSpecs(Collection<CatalogProtos.SortSpecProto> sortSpecProtos) {
    SortSpec [] sortSpecs = new SortSpec[sortSpecProtos.size()];
    int i = 0;
    for (CatalogProtos.SortSpecProto proto : sortSpecProtos) {
      sortSpecs[i++] = new SortSpec(proto);
    }
    return sortSpecs;
  }
}
