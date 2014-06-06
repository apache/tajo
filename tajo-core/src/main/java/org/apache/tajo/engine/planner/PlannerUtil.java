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
import org.apache.tajo.algebra.*;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
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

  public static boolean checkIfDDLPlan(LogicalNode node) {
    LogicalNode baseNode = node;
    if (node instanceof LogicalRootNode) {
      baseNode = ((LogicalRootNode) node).getChild();
    }

    NodeType type = baseNode.getType();

    return
        type == NodeType.CREATE_DATABASE ||
            type == NodeType.DROP_DATABASE ||
            (type == NodeType.CREATE_TABLE && !((CreateTableNode) baseNode).hasSubQuery()) ||
            baseNode.getType() == NodeType.DROP_TABLE ||
            baseNode.getType() == NodeType.ALTER_TABLESPACE ||
            baseNode.getType() == NodeType.ALTER_TABLE ||
            baseNode.getType() == NodeType.TRUNCATE_TABLE;
  }

  /**
   * Checks whether the query is simple or not.
   * The simple query can be defined as 'select * from tb_name [LIMIT X]'.
   *
   * @param plan The logical plan
   * @return True if the query is a simple query.
   */
  public static boolean checkIfSimpleQuery(LogicalPlan plan) {
    LogicalRootNode rootNode = plan.getRootBlock().getRoot();

    // one block, without where clause, no group-by, no-sort, no-join
    boolean isOneQueryBlock = plan.getQueryBlocks().size() == 1;
    boolean simpleOperator = rootNode.getChild().getType() == NodeType.LIMIT
        || rootNode.getChild().getType() == NodeType.SCAN;
    boolean noOrderBy = !plan.getRootBlock().hasNode(NodeType.SORT);
    boolean noGroupBy = !plan.getRootBlock().hasNode(NodeType.GROUP_BY);
    boolean noWhere = !plan.getRootBlock().hasNode(NodeType.SELECTION);
    boolean noJoin = !plan.getRootBlock().hasNode(NodeType.JOIN);
    boolean singleRelation = plan.getRootBlock().hasNode(NodeType.SCAN)
        && PlannerUtil.getRelationLineage(plan.getRootBlock().getRoot()).length == 1;

    boolean noComplexComputation = false;
    if (singleRelation) {
      ScanNode scanNode = plan.getRootBlock().getNode(NodeType.SCAN);
      if (!scanNode.getTableDesc().hasPartition() && scanNode.hasTargets()
          && scanNode.getTargets().length == scanNode.getInSchema().size()) {
        noComplexComputation = true;
        for (int i = 0; i < scanNode.getTargets().length; i++) {
          noComplexComputation = noComplexComputation && scanNode.getTargets()[i].getEvalTree().getType() == EvalType.FIELD;
          if (noComplexComputation) {
            noComplexComputation = noComplexComputation && scanNode.getTargets()[i].getNamedColumn().equals(scanNode.getInSchema().getColumn(i));
          }
          if (!noComplexComputation) {
            return noComplexComputation;
          }
        }
      }
    }

    return !checkIfDDLPlan(rootNode) &&
        (simpleOperator && noComplexComputation  && isOneQueryBlock && noOrderBy && noGroupBy && noWhere && noJoin && singleRelation);
  }

  /**
   * Checks whether the query has 'from clause' or not.
   *
   * @param plan The logical plan
   * @return True if a query does not have 'from clause'.
   */
  public static boolean checkIfNonFromQuery(LogicalPlan plan) {
    LogicalNode node = plan.getRootBlock().getRoot();

    // one block, without where clause, no group-by, no-sort, no-join
    boolean isOneQueryBlock = plan.getQueryBlocks().size() == 1;
    boolean noRelation = !plan.getRootBlock().hasAlgebraicExpr(OpType.Relation);

    return !checkIfDDLPlan(node) && noRelation && isOneQueryBlock;
  }

  /**
   * Get all RelationNodes which are descendant of a given LogicalNode.
   *
   * @param from The LogicalNode to start visiting LogicalNodes.
   * @return an array of all descendant RelationNode of LogicalNode.
   */
  public static String[] getRelationLineage(LogicalNode from) {
    LogicalNode[] scans = findAllNodes(from, NodeType.SCAN, NodeType.PARTITIONS_SCAN);
    String[] tableNames = new String[scans.length];
    ScanNode scan;
    for (int i = 0; i < scans.length; i++) {
      scan = (ScanNode) scans[i];
      tableNames[i] = scan.getCanonicalName();
    }
    return tableNames;
  }

  /**
   * Get all RelationNodes which are descendant of a given LogicalNode.
   * The finding is restricted within a query block.
   *
   * @param from The LogicalNode to start visiting LogicalNodes.
   * @return an array of all descendant RelationNode of LogicalNode.
   */
  public static Collection<String> getRelationLineageWithinQueryBlock(LogicalPlan plan, LogicalNode from)
      throws PlanningException {
    RelationFinderVisitor visitor = new RelationFinderVisitor();
    visitor.visit(null, plan, null, from, new Stack<LogicalNode>());
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
   * @param parent      this node must be a parent node of one node to be removed.
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
        QueryBlock block, PartitionedTableScanNode node, Stack<LogicalNode> stack)

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
  public static LogicalNode[] findAllNodes(LogicalNode node, NodeType... type) {
    Preconditions.checkNotNull(node);
    Preconditions.checkNotNull(type);

    LogicalNodeFinder finder = new LogicalNodeFinder(type);
    node.postOrder(finder);

    if (finder.getFoundNodes().size() == 0) {
      return new LogicalNode[]{};
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

  private static class LogicalNodeFinder implements LogicalNodeVisitor {
    private List<LogicalNode> list = new ArrayList<LogicalNode>();
    private final NodeType[] tofind;
    private boolean topmost = false;
    private boolean finished = false;

    public LogicalNodeFinder(NodeType... type) {
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

    public LogicalNode[] getFoundNodeArray() {
      return list.toArray(new LogicalNode[list.size()]);
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
      } else if (node instanceof BinaryNode) {
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
   * @param schema  to be transformed to targets
   * @param targets to be filled
   */
  public static void schemaToTargets(Schema schema, Target[] targets) {
    FieldEval eval;
    for (int i = 0; i < schema.size(); i++) {
      eval = new FieldEval(schema.getColumn(i));
      targets[i] = new Target(eval);
    }
  }

  public static Target[] schemaToTargets(Schema schema) {
    Target[] targets = new Target[schema.size()];

    FieldEval eval;
    for (int i = 0; i < schema.size(); i++) {
      eval = new FieldEval(schema.getColumn(i));
      targets[i] = new Target(eval);
    }
    return targets;
  }

  public static Target[] schemaToTargetsWithGeneratedFields(Schema schema) {
    List<Target> targets = TUtil.newList();

    FieldEval eval;
    for (int i = 0; i < schema.size(); i++) {
      eval = new FieldEval(schema.getColumn(i));
      targets.add(new Target(eval));
    }
    return targets.toArray(new Target[targets.size()]);
  }

  public static SortSpec[] schemaToSortSpecs(Schema schema) {
    return columnsToSortSpecs(schema.toArray());
  }

  public static SortSpec[] columnsToSortSpecs(Column[] columns) {
    SortSpec[] specs = new SortSpec[columns.length];

    for (int i = 0; i < columns.length; i++) {
      specs[i] = new SortSpec(columns[i], true, false);
    }

    return specs;
  }

  public static SortSpec[] columnsToSortSpecs(Collection<Column> columns) {
    return columnsToSortSpecs(columns.toArray(new Column[columns.size()]));
  }

  public static Schema sortSpecsToSchema(SortSpec[] sortSpecs) {
    Schema schema = new Schema();
    for (SortSpec spec : sortSpecs) {
      schema.addColumn(spec.getSortKey());
    }

    return schema;
  }

  public static SortSpec[][] getSortKeysFromJoinQual(EvalNode joinQual, Schema outer, Schema inner) {
    // It is used for the merge join executor. The merge join only considers the equi-join.
    // So, theta-join flag must be false.
    List<Column[]> joinKeyPairs = getJoinKeyPairs(joinQual, outer, inner, false);
    SortSpec[] outerSortSpec = new SortSpec[joinKeyPairs.size()];
    SortSpec[] innerSortSpec = new SortSpec[joinKeyPairs.size()];

    for (int i = 0; i < joinKeyPairs.size(); i++) {
      outerSortSpec[i] = new SortSpec(joinKeyPairs.get(i)[0]);
      innerSortSpec[i] = new SortSpec(joinKeyPairs.get(i)[1]);
    }

    return new SortSpec[][]{outerSortSpec, innerSortSpec};
  }

  public static TupleComparator[] getComparatorsFromJoinQual(EvalNode joinQual, Schema leftSchema, Schema rightSchema) {
    SortSpec[][] sortSpecs = getSortKeysFromJoinQual(joinQual, leftSchema, rightSchema);
    TupleComparator[] comparators = new TupleComparator[2];
    comparators[0] = new TupleComparator(leftSchema, sortSpecs[0]);
    comparators[1] = new TupleComparator(rightSchema, sortSpecs[1]);
    return comparators;
  }

  /**
   * @return the first array contains left table's columns, and the second array contains right table's columns.
   */
  public static Column[][] joinJoinKeyForEachTable(EvalNode joinQual, Schema leftSchema,
                                                   Schema rightSchema, boolean includeThetaJoin) {
    List<Column[]> joinKeys = getJoinKeyPairs(joinQual, leftSchema, rightSchema, includeThetaJoin);
    Column[] leftColumns = new Column[joinKeys.size()];
    Column[] rightColumns = new Column[joinKeys.size()];
    for (int i = 0; i < joinKeys.size(); i++) {
      leftColumns[i] = joinKeys.get(i)[0];
      rightColumns[i] = joinKeys.get(i)[1];
    }

    return new Column[][]{leftColumns, rightColumns};
  }

  public static List<Column[]> getJoinKeyPairs(EvalNode joinQual, Schema leftSchema, Schema rightSchema,
                                               boolean includeThetaJoin) {
    JoinKeyPairFinder finder = new JoinKeyPairFinder(includeThetaJoin, leftSchema, rightSchema);
    joinQual.preOrder(finder);
    return finder.getPairs();
  }

  public static class JoinKeyPairFinder implements EvalNodeVisitor {
    private boolean includeThetaJoin;
    private final List<Column[]> pairs = Lists.newArrayList();
    private Schema[] schemas = new Schema[2];

    public JoinKeyPairFinder(boolean includeThetaJoin, Schema outer, Schema inner) {
      this.includeThetaJoin = includeThetaJoin;
      schemas[0] = outer;
      schemas[1] = inner;
    }

    @Override
    public void visit(EvalNode node) {
      if (EvalTreeUtil.isJoinQual(node, includeThetaJoin)) {
        BinaryEval binaryEval = (BinaryEval) node;
        Column[] pair = new Column[2];

        for (int i = 0; i <= 1; i++) { // access left, right sub expression
          Column column = EvalTreeUtil.findAllColumnRefs(binaryEval.getExpr(i)).get(0);
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

    public List<Column[]> getPairs() {
      return this.pairs;
    }
  }

  public static Schema targetToSchema(Collection<Target> targets) {
    return targetToSchema(targets.toArray(new Target[targets.size()]));
  }

  public static Schema targetToSchema(Target[] targets) {
    Schema schema = new Schema();
    for (Target t : targets) {
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
  public static Target[] stripTarget(Target[] sourceTargets) {
    Target[] copy = new Target[sourceTargets.length];
    for (int i = 0; i < sourceTargets.length; i++) {
      try {
        copy[i] = (Target) sourceTargets[i].clone();
      } catch (CloneNotSupportedException e) {
        throw new InternalError(e.getMessage());
      }
      if (copy[i].getEvalTree().getType() == EvalType.FIELD) {
        FieldEval fieldEval = copy[i].getEvalTree();
        if (fieldEval.getColumnRef().hasQualifier()) {
          fieldEval.replaceColumnRef(fieldEval.getColumnName());
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
        if (node instanceof DistinctGroupbyNode) {
          DistinctGroupbyNode dNode = (DistinctGroupbyNode)copy;
          for (GroupbyNode eachNode: dNode.getGroupByNodes()) {
            eachNode.setPID(plan.newPID());
          }
        }
      }
      return copy;
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  public static boolean isCommutativeJoin(JoinType joinType) {
    return joinType == JoinType.INNER;
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
      String[] parts = n.split("\\.");
      if (parts.length == 1) {
        names.add(qualifier + "." + parts[0]);
      } else {
        names.add(qualifier + "." + parts[1]);
      }
    }
    return names;
  }

  public static SortSpec[] convertSortSpecs(Collection<CatalogProtos.SortSpecProto> sortSpecProtos) {
    SortSpec[] sortSpecs = new SortSpec[sortSpecProtos.size()];
    int i = 0;
    for (CatalogProtos.SortSpecProto proto : sortSpecProtos) {
      sortSpecs[i++] = new SortSpec(proto);
    }
    return sortSpecs;
  }

  /**
   * Generate an explain string of a LogicalNode and its descendant nodes.
   *
   * @param node The LogicalNode instance to be started
   * @return A pretty print explain string
   */
  public static String buildExplainString(LogicalNode node) {
    ExplainLogicalPlanVisitor explain = new ExplainLogicalPlanVisitor();

    StringBuilder explains = new StringBuilder();
    try {
      ExplainLogicalPlanVisitor.Context explainContext = explain.getBlockPlanStrings(null, node);
      while (!explainContext.explains.empty()) {
        explains.append(
            ExplainLogicalPlanVisitor.printDepthString(explainContext.getMaxDepth(), explainContext.explains.pop()));
      }
    } catch (PlanningException e) {
      throw new RuntimeException(e);
    }

    return explains.toString();
  }
}
