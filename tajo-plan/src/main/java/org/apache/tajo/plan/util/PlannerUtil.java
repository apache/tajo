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
import org.apache.tajo.OverridableConf;
import org.apache.tajo.SessionVars;
import org.apache.tajo.algebra.*;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.plan.*;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;
import org.apache.tajo.plan.visitor.ExplainLogicalPlanVisitor;
import org.apache.tajo.plan.visitor.SimpleAlgebraVisitor;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.TUtil;

import java.io.IOException;
import java.util.*;

import static org.apache.tajo.catalog.proto.CatalogProtos.StoreType.CSV;
import static org.apache.tajo.catalog.proto.CatalogProtos.StoreType.TEXTFILE;

public class PlannerUtil {

  public static final Column [] EMPTY_COLUMNS = new Column[] {};
  public static final AggregationFunctionCallEval [] EMPTY_AGG_FUNCS = new AggregationFunctionCallEval[] {};

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
        || rootNode.getChild().getType() == NodeType.SCAN || rootNode.getChild().getType() == NodeType.PARTITIONS_SCAN;
    boolean noOrderBy = !plan.getRootBlock().hasNode(NodeType.SORT);
    boolean noGroupBy = !plan.getRootBlock().hasNode(NodeType.GROUP_BY);
    boolean noWhere = !plan.getRootBlock().hasNode(NodeType.SELECTION);
    boolean noJoin = !plan.getRootBlock().hasNode(NodeType.JOIN);
    boolean singleRelation =
        (plan.getRootBlock().hasNode(NodeType.SCAN) || plan.getRootBlock().hasNode(NodeType.PARTITIONS_SCAN)) &&
        PlannerUtil.getRelationLineage(plan.getRootBlock().getRoot()).length == 1;

    boolean noComplexComputation = false;
    boolean prefixPartitionWhere = false;
    if (singleRelation) {
      ScanNode scanNode = plan.getRootBlock().getNode(NodeType.SCAN);
      if (scanNode == null) {
        scanNode = plan.getRootBlock().getNode(NodeType.PARTITIONS_SCAN);
      }
      if (scanNode.hasTargets()) {
        // If the number of columns in the select clause is s different from table schema,
        // This query is not a simple query.
        if (scanNode.getTableDesc().hasPartition()) {
          // In the case of partitioned table, the actual number of columns is ScanNode.InSchema + partitioned columns
          int numPartitionColumns = scanNode.getTableDesc().getPartitionMethod().getExpressionSchema().size();
          if (scanNode.getTargets().length != scanNode.getInSchema().size() + numPartitionColumns) {
            return false;
          }
        } else {
          if (scanNode.getTargets().length != scanNode.getInSchema().size()) {
            return false;
          }
        }
        noComplexComputation = true;
        for (int i = 0; i < scanNode.getTargets().length; i++) {
          noComplexComputation =
              noComplexComputation && scanNode.getTargets()[i].getEvalTree().getType() == EvalType.FIELD;
          if (noComplexComputation) {
            noComplexComputation = noComplexComputation &&
                scanNode.getTargets()[i].getNamedColumn().equals(
                    scanNode.getTableDesc().getLogicalSchema().getColumn(i));
          }
          if (!noComplexComputation) {
            return noComplexComputation;
          }
        }
      }

      /**
       * TODO: Remove isExternal check after resolving the following issues
       * - TAJO-1416: INSERT INTO EXTERNAL PARTITIONED TABLE
       * - TAJO-1441: INSERT INTO MANAGED PARTITIONED TABLE
       */
      if (!noWhere && scanNode.getTableDesc().isExternal() && scanNode.getTableDesc().getPartitionMethod() != null) {
        EvalNode node = ((SelectionNode) plan.getRootBlock().getNode(NodeType.SELECTION)).getQual();
        Schema partSchema = scanNode.getTableDesc().getPartitionMethod().getExpressionSchema();
        if (EvalTreeUtil.checkIfPartitionSelection(node, partSchema)) {
          prefixPartitionWhere = true;
          boolean isPrefix = true;
          for (Column c : partSchema.getRootColumns()) {
            String value = EvalTreeUtil.getPartitionValue(node, c.getSimpleName());
            if (isPrefix && value == null)
              isPrefix = false;
            else if (!isPrefix && value != null) {
              prefixPartitionWhere = false;
              break;
            }
          }
        }
      }
    }

    return !checkIfDDLPlan(rootNode) &&
        (simpleOperator && noComplexComputation && isOneQueryBlock &&
            noOrderBy && noGroupBy && (noWhere || prefixPartitionWhere) && noJoin && singleRelation);
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
      isVirtualTable &= (scanNode.getTableDesc().getMeta().getStoreType().equalsIgnoreCase("SYSTEM"));
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

  public static String getTopRelationInLineage(LogicalPlan plan, LogicalNode from) throws PlanningException {
    RelationFinderVisitor visitor = new RelationFinderVisitor(true);
    visitor.visit(null, plan, null, from, new Stack<LogicalNode>());
    if (visitor.getFoundRelations().isEmpty()) {
      return null;
    } else {
      return visitor.getFoundRelations().iterator().next();
    }
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
    RelationFinderVisitor visitor = new RelationFinderVisitor(false);
    visitor.visit(null, plan, null, from, new Stack<LogicalNode>());
    return visitor.getFoundRelations();
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
                             Stack<LogicalNode> stack) throws PlanningException {
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

  public static boolean isCommutativeJoin(JoinType joinType) {
    return joinType == JoinType.INNER;
  }

  public static boolean isOuterJoin(JoinType joinType) {
    return joinType == JoinType.LEFT_OUTER || joinType == JoinType.RIGHT_OUTER || joinType==JoinType.FULL_OUTER;
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

  public static void applySessionToTableProperties(OverridableConf sessionVars,
                                                   String storeType,
                                                   KeyValueSet tableProperties) {
    if (storeType.equalsIgnoreCase("CSV") || storeType.equalsIgnoreCase("TEXT")) {
      if (sessionVars.containsKey(SessionVars.NULL_CHAR)) {
        tableProperties.set(StorageConstants.TEXT_NULL, sessionVars.get(SessionVars.NULL_CHAR));
      }

      if (sessionVars.containsKey(SessionVars.TIMEZONE)) {
        tableProperties.set(StorageConstants.TIMEZONE, sessionVars.get(SessionVars.TIMEZONE));
      }
    }
  }

  /**
   * This method sets a set of table properties by System default configs.
   * These properties are implicitly used to read or write rows in Table.
   * Don't use this method for TableMeta to be stored in Catalog.
   *
   * @param systemConf System configuration
   * @param meta TableMeta to be set
   */
  public static void applySystemDefaultToTableProperties(OverridableConf systemConf, TableMeta meta) {
    if (!meta.containsOption(StorageConstants.TIMEZONE)) {
      meta.putOption(StorageConstants.TIMEZONE, systemConf.get(SessionVars.TIMEZONE));
    }
  }

  public static boolean isFileStorageType(String storageType) {
    if (storageType.equalsIgnoreCase("hbase")) {
      return false;
    } else {
      return true;
    }
  }

  public static String getStoreType(LogicalPlan plan) {
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

  public static TableDesc getTableDesc(CatalogService catalog, LogicalNode node) throws IOException {
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
      if (insertNode.getPath() != null) {
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
            createTableNode.getPath() != null ? createTableNode.getPath().toUri() : null);

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
            insertNode.getPath() != null ? insertNode.getPath().toUri() : null);

    if (insertNode.hasPartition()) {
      tableDescTobeCreated.setPartitionMethod(insertNode.getPartitionMethod());
    }

    return tableDescTobeCreated;
  }
}
