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

package org.apache.tajo.plan.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.tajo.algebra.*;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.*;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.exception.UndefinedTableException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.serder.PlanProto.ShuffleType;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;
import org.apache.tajo.plan.visitor.ExplainLogicalPlanVisitor;
import org.apache.tajo.plan.visitor.SimpleAlgebraVisitor;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.StringUtils;
import org.apache.tajo.util.TUtil;

import java.util.*;

public class PlannerUtil {

  public static final Column [] EMPTY_COLUMNS = new Column[] {};
  public static final List<AggregationFunctionCallEval> EMPTY_AGG_FUNCS = new ArrayList<>();

  public static boolean checkIfSetSession(LogicalNode node) {
    LogicalNode baseNode = node;
    if (node instanceof LogicalRootNode) {
      baseNode = ((LogicalRootNode) node).getChild();
    }

    return baseNode.getType() == NodeType.SET_SESSION;

  }

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
            type == NodeType.DROP_TABLE ||
            type == NodeType.ALTER_TABLESPACE ||
            type == NodeType.ALTER_TABLE ||
            type == NodeType.TRUNCATE_TABLE ||
            type == NodeType.CREATE_INDEX ||
            type == NodeType.DROP_INDEX;
  }

  /**
   * Most update queries require only the updates to the catalog information,
   * but some queries such as "CREATE INDEX" or CTAS requires distributed execution on multiple cluster nodes.
   * This function checks whether the given DDL plan requires distributed execution or not.
   * @param node the root node of a query plan
   * @return Return true if the input query plan requires distributed execution. Otherwise, return false.
   */
  public static boolean isDistExecDDL(LogicalNode node) {
    LogicalNode baseNode = node;
    if (node instanceof LogicalRootNode) {
      baseNode = ((LogicalRootNode) node).getChild();
    }

    NodeType type = baseNode.getType();

    return type == NodeType.CREATE_INDEX && !((CreateIndexNode)baseNode).isExternal() ||
        type == NodeType.CREATE_TABLE && ((CreateTableNode)baseNode).hasSubQuery();
  }

  /**
   * Checks whether the query is simple or not.
   *
   * The simple query can be as follows:
   * <ul>
   * <li><code>'select * from tb_name [LIMIT X]'</code></li>
   * <li><code>'select length(name), name from tb_name [LIMIT X]'</code></li>
   * </ul>
   *
   * @param plan The logical plan
   * @return True if the query is a simple query.
   */
  public static boolean checkIfSimpleQuery(LogicalPlan plan) {
    LogicalRootNode rootNode = plan.getRootBlock().getRoot();

    // one block, without where clause, no group-by, no-sort, no-join
    boolean isOneQueryBlock = plan.getQueryBlocks().size() == 1;
    boolean simpleOperator = rootNode.getChild().getType() == NodeType.LIMIT
        || rootNode.getChild().getType() == NodeType.SCAN || rootNode.getChild().getType() == NodeType.PARTITIONS_SCAN;
    boolean noOrderBy = !plan.getRootBlock().hasNode(NodeType.SORT);
    boolean noGroupBy = !plan.getRootBlock().hasNode(NodeType.GROUP_BY);
    boolean noWhere = !plan.getRootBlock().hasNode(NodeType.SELECTION);
    boolean noJoin = !plan.getRootBlock().hasNode(NodeType.JOIN);
    boolean singleRelation =
        (plan.getRootBlock().hasNode(NodeType.SCAN) || plan.getRootBlock().hasNode(NodeType.PARTITIONS_SCAN)) &&
        PlannerUtil.getRelationLineage(plan.getRootBlock().getRoot()).length == 1;

    boolean partitionWhere = false;
    if (singleRelation) {
      ScanNode scanNode = plan.getRootBlock().getNode(NodeType.SCAN);
      if (scanNode == null) {
        scanNode = plan.getRootBlock().getNode(NodeType.PARTITIONS_SCAN);
      }

      if (!noWhere && scanNode.getTableDesc().hasPartition()) {
        EvalNode node = ((SelectionNode) plan.getRootBlock().getNode(NodeType.SELECTION)).getQual();
        Schema partSchema = scanNode.getTableDesc().getPartitionMethod().getExpressionSchema();
        if (EvalTreeUtil.checkIfPartitionSelection(node, partSchema)) {
          partitionWhere = true;
        }
      }
    }

    return !checkIfDDLPlan(rootNode) &&
        (simpleOperator && isOneQueryBlock &&
            noOrderBy && noGroupBy && (noWhere || partitionWhere) && noJoin && singleRelation);
  }
  
  /**
   * Checks whether the target of this query is a virtual table or not.
   * It will be removed after tajo storage supports catalog service access.
   * 
   */
  public static boolean checkIfQueryTargetIsVirtualTable(LogicalPlan plan) {
    LogicalRootNode rootNode = plan.getRootBlock().getRoot();
    
    boolean hasScanNode = plan.getRootBlock().hasNode(NodeType.SCAN);
    LogicalNode[] scanNodes = findAllNodes(rootNode, NodeType.SCAN);
    boolean isVirtualTable = scanNodes.length > 0;
    ScanNode scanNode = null;
    
    for (LogicalNode node: scanNodes) {
      scanNode = (ScanNode) node;
      isVirtualTable &= (scanNode.getTableDesc().getMeta().getDataFormat().equalsIgnoreCase("SYSTEM"));
    }
    
    return !checkIfDDLPlan(rootNode) && hasScanNode && isVirtualTable;
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

  public static String getTopRelationInLineage(LogicalPlan plan, LogicalNode from) throws TajoException {
    RelationFinderVisitor visitor = new RelationFinderVisitor(true);
    visitor.visit(null, plan, null, from, new Stack<>());
    if (visitor.getFoundRelations().isEmpty()) {
      return null;
    } else {
      return visitor.getFoundRelations().iterator().next();
    }
  }

  public static class RelationFinderVisitor extends BasicLogicalPlanVisitor<Object, LogicalNode> {
    private Set<String> foundRelNameSet = Sets.newHashSet();
    private boolean topOnly = false;

    public RelationFinderVisitor(boolean topOnly) {
      this.topOnly = topOnly;
    }

    public Set<String> getFoundRelations() {
      return foundRelNameSet;
    }

    @Override
    public LogicalNode visit(Object context, LogicalPlan plan, @Nullable LogicalPlan.QueryBlock block, LogicalNode node,
                             Stack<LogicalNode> stack) throws TajoException {
      if (topOnly && foundRelNameSet.size() > 0) {
        return node;
      }

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
        throw new TajoInternalError("both logical node must be parent and child nodes");
      }
    } else {
      throw new TajoInternalError("unexpected logical plan: " + parent);
    }
    return child;
  }

  public static void replaceNode(LogicalPlan plan, LogicalNode startNode, LogicalNode oldNode, LogicalNode newNode) {
    LogicalNodeReplaceVisitor replacer = new LogicalNodeReplaceVisitor(oldNode, newNode);
    try {
      replacer.visit(new ReplacerContext(), plan, null, startNode, new Stack<>());
    } catch (TajoException e) {
      throw new TajoInternalError(e);
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
                             LogicalNode node, Stack<LogicalNode> stack) throws TajoException {
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
                                 Stack<LogicalNode> stack) throws TajoException {
      return node;
    }

    @Override
    public LogicalNode visitPartitionedTableScan(ReplacerContext context, LogicalPlan plan, LogicalPlan.
        QueryBlock block, PartitionedTableScanNode node, Stack<LogicalNode> stack) {
      return node;
    }

    @Override
    public LogicalNode visitIndexScan(ReplacerContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                      IndexScanNode node, Stack<LogicalNode> stack) throws TajoException {
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
    node.preOrder(finder);

    if (finder.getFoundNodes().size() == 0) {
      return null;
    }
    return (T) finder.getFoundNodes().get(0);
  }

  private static class LogicalNodeFinder implements LogicalNodeVisitor {
    private List<LogicalNode> list = new ArrayList<>();
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
    private List<LogicalNode> list = new ArrayList<>();
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

  public static List<Target> schemaToTargets(Schema schema) {
    List<Target> targets = new ArrayList<>();

    FieldEval eval;
    for (int i = 0; i < schema.size(); i++) {
      eval = new FieldEval(schema.getColumn(i));
      targets.add(new Target(eval));
    }
    return targets;
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
      if (EvalTreeUtil.isJoinQual(null, schemas[0], schemas[1], node, includeThetaJoin)) {
        BinaryEval binaryEval = (BinaryEval) node;
        Column[] pair = new Column[2];

        for (int i = 0; i <= 1; i++) { // access left, right sub expression
          Column column = EvalTreeUtil.findAllColumnRefs(binaryEval.getChild(i)).get(0);
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

    public List<Column[]> getPairs() {
      return this.pairs;
    }
  }

  public static Schema targetToSchema(Collection<Target> targets) {
    return targetToSchema(targets);
  }

  public static Schema targetToSchema(List<Target> targets) {
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
  public static List<Target> stripTarget(List<Target> sourceTargets) {
    List<Target> copy = new ArrayList<>();
    for (int i = 0; i < sourceTargets.size(); i++) {
      try {
        copy.add((Target) sourceTargets.get(i).clone());
      } catch (CloneNotSupportedException e) {
        throw new InternalError(e.getMessage());
      }
      if (copy.get(i).getEvalTree().getType().equals(EvalType.FIELD)) {
        FieldEval fieldEval = copy.get(i).getEvalTree();
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
          for (GroupbyNode eachNode: dNode.getSubPlans()) {
            eachNode.setPID(plan.newPID());
          }
        }
      }
      return copy;
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  public static boolean isCommutativeJoinType(JoinType joinType) {
    // Full outer join is also commutative.
    return joinType == JoinType.INNER || joinType == JoinType.CROSS || joinType == JoinType.FULL_OUTER;
  }

  public static boolean isOuterJoinType(JoinType joinType) {
    return joinType == JoinType.LEFT_OUTER || joinType == JoinType.RIGHT_OUTER || joinType==JoinType.FULL_OUTER;
  }

  public static boolean existsAggregationFunction(Expr expr) throws TajoException {
    AggregationFunctionFinder finder = new AggregationFunctionFinder();
    AggFunctionFoundResult result = new AggFunctionFoundResult();
    finder.visit(result, new Stack<>(), expr);
    return result.generalSetFunction;
  }

  public static boolean existsDistinctAggregationFunction(Expr expr) throws TajoException {
    AggregationFunctionFinder finder = new AggregationFunctionFinder();
    AggFunctionFoundResult result = new AggFunctionFoundResult();
    finder.visit(result, new Stack<>(), expr);
    return result.distinctSetFunction;
  }

  static class AggFunctionFoundResult {
    boolean generalSetFunction;
    boolean distinctSetFunction;
  }

  static class AggregationFunctionFinder extends SimpleAlgebraVisitor<AggFunctionFoundResult, Object> {
    @Override
    public Object visitCountRowsFunction(AggFunctionFoundResult ctx, Stack<Expr> stack, CountRowsFunctionExpr expr)
        throws TajoException {
      ctx.generalSetFunction = true;
      return super.visitCountRowsFunction(ctx, stack, expr);
    }

    @Override
    public Object visitGeneralSetFunction(AggFunctionFoundResult ctx, Stack<Expr> stack, GeneralSetFunctionExpr expr)
        throws TajoException {
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
    } catch (TajoException e) {
      throw new TajoInternalError(e);
    }

    return explains.toString();
  }

  public static String getShuffleType(ShuffleType shuffleType) {
    if (shuffleType == null) return ShuffleType.NONE_SHUFFLE.toString();
    return shuffleType.toString();
  }

  public static ShuffleType getShuffleType(String shuffleType) {
    if (StringUtils.isEmpty(shuffleType)) return ShuffleType.NONE_SHUFFLE;
    return ShuffleType.valueOf(shuffleType);
  }

  public static boolean isFileStorageType(String storageType) {
    if (storageType.equalsIgnoreCase("hbase")) {
      return false;
    } else {
      return true;
    }
  }

  public static String getDataFormat(LogicalPlan plan) {
    LogicalRootNode rootNode = plan.getRootBlock().getRoot();
    NodeType nodeType = rootNode.getChild().getType();
    if (nodeType == NodeType.CREATE_TABLE) {
      return ((CreateTableNode)rootNode.getChild()).getStorageType();
    } else if (nodeType == NodeType.INSERT) {
      return ((InsertNode)rootNode.getChild()).getStorageType();
    } else {
      return null;
    }
  }

  public static String getStoreTableName(LogicalPlan plan) {
    LogicalRootNode rootNode = plan.getRootBlock().getRoot();
    NodeType nodeType = rootNode.getChild().getType();
    if (nodeType == NodeType.CREATE_TABLE) {
      return ((CreateTableNode)rootNode.getChild()).getTableName();
    } else if (nodeType == NodeType.INSERT) {
      return ((InsertNode)rootNode.getChild()).getTableName();
    } else {
      return null;
    }
  }

  public static TableDesc getOutputTableDesc(LogicalPlan plan) {
    LogicalNode [] found = findAllNodes(plan.getRootNode().getChild(), NodeType.CREATE_TABLE, NodeType.INSERT);

    if (found.length == 0) {
      return new TableDesc(null, plan.getRootNode().getOutSchema(), "TEXT", new KeyValueSet(), null);
    } else {
      StoreTableNode storeNode = (StoreTableNode) found[0];
      return new TableDesc(
          storeNode.getTableName(),
          storeNode.getOutSchema(),
          storeNode.getStorageType(),
          storeNode.getOptions(),
          storeNode.getUri());
    }
  }

  public static TableDesc getTableDesc(CatalogService catalog, LogicalNode node) throws UndefinedTableException {
    if (node.getType() == NodeType.ROOT) {
      node = ((LogicalRootNode)node).getChild();
    }

    if (node.getType() == NodeType.CREATE_TABLE) {
      return createTableDesc((CreateTableNode)node);
    }
    String tableName = null;
    InsertNode insertNode = null;
    if (node.getType() == NodeType.INSERT) {
      insertNode = (InsertNode)node;
      tableName = insertNode.getTableName();
    } else {
      return null;
    }

    if (tableName != null) {
      String[] tableTokens = tableName.split("\\.");
      if (tableTokens.length >= 2) {
        if (catalog.existsTable(tableTokens[0], tableTokens[1])) {
          return catalog.getTableDesc(tableTokens[0], tableTokens[1]);
        }
      }
    } else {
      if (insertNode.getUri() != null) {
        //insert ... location
        return createTableDesc(insertNode);
      }
    }
    return null;
  }

  private static TableDesc createTableDesc(CreateTableNode createTableNode) {
    TableMeta meta = new TableMeta(createTableNode.getStorageType(), createTableNode.getOptions());

    TableDesc tableDescTobeCreated =
        new TableDesc(
            createTableNode.getTableName(),
            createTableNode.getTableSchema(),
            meta,
            createTableNode.getUri() != null ? createTableNode.getUri() : null);

    tableDescTobeCreated.setExternal(createTableNode.isExternal());

    if (createTableNode.hasPartition()) {
      tableDescTobeCreated.setPartitionMethod(createTableNode.getPartitionMethod());
    }

    return tableDescTobeCreated;
  }

  private static TableDesc createTableDesc(InsertNode insertNode) {
    TableMeta meta = new TableMeta(insertNode.getStorageType(), insertNode.getOptions());

    TableDesc tableDescTobeCreated =
        new TableDesc(
            insertNode.getTableName(),
            insertNode.getTableSchema(),
            meta,
            insertNode.getUri() != null ? insertNode.getUri() : null);

    if (insertNode.hasPartition()) {
      tableDescTobeCreated.setPartitionMethod(insertNode.getPartitionMethod());
    }

    return tableDescTobeCreated;
  }

  /**
   * Extract all in-subqueries from the given qual.
   *
   * @param qual
   * @return
   */
  public static List<Expr> extractInSubquery(Expr qual) {
    List<Expr> inSubqueries = TUtil.newList();
    for (Expr eachIn : ExprFinder.findsInOrder(qual, OpType.InPredicate)) {
      InPredicate inPredicate = (InPredicate) eachIn;
      if (inPredicate.getInValue().getType() == OpType.SimpleTableSubquery) {
        inSubqueries.add(eachIn);
      }
    }
    return inSubqueries;
  }

  /**
   * Return a list of integers, maps input schema and projected columns.
   * Each integer value means a column index of input schema corresponding to each project column
   *
   * @param inputSchema Input Schema
   * @param targets Columns to be projected
   * @return A list of integers, each of which is an index number of input schema corresponding
   *         to each projected column.
   */
  public static int [] getTargetIds(Schema inputSchema, Column...targets) {
    int [] targetIds = new int[targets.length];
    for (int i = 0; i < targetIds.length; i++) {
      targetIds[i] = inputSchema.getColumnId(targets[i].getQualifiedName());
    }
    Arrays.sort(targetIds);

    return targetIds;
  }

  public static List<EvalNode> getAllEqualEvals(EvalNode qual) {
    EvalTreeUtil.EvalFinder finder = new EvalTreeUtil.EvalFinder(EvalType.EQUAL);
    finder.visit(null, qual, new Stack<>());
    return finder.getEvalNodes();
  }

  public static boolean hasAsterisk(List<NamedExpr> namedExprs) {
    for (NamedExpr eachTarget : namedExprs) {
      if (eachTarget.getExpr().getType() == OpType.Asterisk) {
        return true;
      }
    }
    return false;
  }
}
