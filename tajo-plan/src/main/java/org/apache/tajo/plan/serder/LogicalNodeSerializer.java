/*
 * Lisensed to the Apache Software Foundation (ASF) under one
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

package org.apache.tajo.plan.serder;

import com.google.common.collect.Maps;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.exception.UnimplementedException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.serder.PlanProto.AlterTableNode.AddColumn;
import org.apache.tajo.plan.serder.PlanProto.AlterTableNode.RenameColumn;
import org.apache.tajo.plan.serder.PlanProto.AlterTableNode.RenameTable;
import org.apache.tajo.plan.serder.PlanProto.AlterTablespaceNode.SetLocation;
import org.apache.tajo.plan.serder.PlanProto.LogicalNodeTree;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;
import org.apache.tajo.util.ProtoUtil;
import org.apache.tajo.util.TUtil;

import java.util.List;
import java.util.Map;
import java.util.Stack;

/**
 * It serializes a logical plan into a protobuf-based serialized bytes.
 *
 * In detail, it traverses all logical nodes in a postfix order.
 * For each visiting node, it serializes the node and adds the serialized bytes into a list.
 * Then, a list will contains a list of serialized nodes in a postfix order.
 *
 * @see org.apache.tajo.plan.serder.LogicalNodeDeserializer
 */
public class LogicalNodeSerializer extends BasicLogicalPlanVisitor<LogicalNodeSerializer.SerializeContext,
    LogicalNode> {

  private static final LogicalNodeSerializer instance;

  static {
    instance = new LogicalNodeSerializer();
  }

  /**
   * Serialize a logical plan into a protobuf-based serialized bytes.
   *
   * @param node LogicalNode to be serialized
   * @return A list of serialized nodes
   */
  public static LogicalNodeTree serialize(LogicalNode node) {
    SerializeContext context = new SerializeContext();
    try {
      instance.visit(context, null, null, node, new Stack<LogicalNode>());
    } catch (PlanningException e) {
      throw new RuntimeException(e);
    }
    return context.treeBuilder.build();
  }

  private static PlanProto.LogicalNode.Builder createNodeBuilder(SerializeContext context, LogicalNode node) {
    int selfId;
    if (context.idMap.containsKey(node)) {
      selfId = context.idMap.get(node);
    } else {
      selfId = context.seqId++;
      context.idMap.put(node, selfId);
    }

    PlanProto.LogicalNode.Builder nodeBuilder = PlanProto.LogicalNode.newBuilder();
    nodeBuilder.setVisitSeq(selfId);
    nodeBuilder.setNodeId(node.getPID());
    nodeBuilder.setType(convertType(node.getType()));

    // some DDL statements like DropTable or DropDatabase do not have in/out schemas
    if (node.getInSchema() != null) {
      nodeBuilder.setInSchema(node.getInSchema().getProto());
    }
    if (node.getOutSchema() != null) {
      nodeBuilder.setOutSchema(node.getOutSchema().getProto());
    }
    return nodeBuilder;
  }

  public static class SerializeContext {
    private int seqId = 0;
    private Map<LogicalNode, Integer> idMap = Maps.newHashMap();
    private LogicalNodeTree.Builder treeBuilder = LogicalNodeTree.newBuilder();
  }

  public LogicalNode visitRoot(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                               LogicalRootNode root, Stack<LogicalNode> stack) throws PlanningException {
    super.visitRoot(context, plan, block, root, stack);

    int [] childIds = registerGetChildIds(context, root);

    PlanProto.RootNode.Builder rootBuilder = PlanProto.RootNode.newBuilder();
    rootBuilder.setChildSeq(childIds[0]);

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, root);
    nodeBuilder.setRoot(rootBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return root;
  }

  @Override
  public LogicalNode visitSetSession(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                     SetSessionNode node, Stack<LogicalNode> stack) throws PlanningException {
    super.visitSetSession(context, plan, block, node, stack);

    PlanProto.SetSessionNode.Builder builder = PlanProto.SetSessionNode.newBuilder();
    builder.setName(node.getName());
    if (node.hasValue()) {
      builder.setValue(node.getValue());
    }

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, node);
    nodeBuilder.setSetSession(builder);
    context.treeBuilder.addNodes(nodeBuilder);

    return node;
  }

  public LogicalNode visitEvalExpr(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                   EvalExprNode exprEval, Stack<LogicalNode> stack) throws PlanningException {
    PlanProto.EvalExprNode.Builder exprEvalBuilder = PlanProto.EvalExprNode.newBuilder();
    exprEvalBuilder.addAllTargets(
        ProtoUtil.<PlanProto.Target>toProtoObjects(exprEval.getTargets()));

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, exprEval);
    nodeBuilder.setExprEval(exprEvalBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return exprEval;
  }

  public LogicalNode visitProjection(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                     ProjectionNode projection, Stack<LogicalNode> stack) throws PlanningException {
    super.visitProjection(context, plan, block, projection, stack);

    int [] childIds = registerGetChildIds(context, projection);

    PlanProto.ProjectionNode.Builder projectionBuilder = PlanProto.ProjectionNode.newBuilder();
    projectionBuilder.setChildSeq(childIds[0]);
    projectionBuilder.addAllTargets(
        ProtoUtil.<PlanProto.Target>toProtoObjects(projection.getTargets()));
    projectionBuilder.setDistinct(projection.isDistinct());

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, projection);
    nodeBuilder.setProjection(projectionBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return projection;
  }

  @Override
  public LogicalNode visitLimit(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                LimitNode limit, Stack<LogicalNode> stack) throws PlanningException {
    super.visitLimit(context, plan, block, limit, stack);

    int [] childIds = registerGetChildIds(context, limit);

    PlanProto.LimitNode.Builder limitBuilder = PlanProto.LimitNode.newBuilder();
    limitBuilder.setChildSeq(childIds[0]);
    limitBuilder.setFetchFirstNum(limit.getFetchFirstNum());

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, limit);
    nodeBuilder.setLimit(limitBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return limit;
  }

  public LogicalNode visitWindowAgg(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                    WindowAggNode windowAgg, Stack<LogicalNode> stack) throws PlanningException {
    super.visitWindowAgg(context, plan, block, windowAgg, stack);

    int [] childIds = registerGetChildIds(context, windowAgg);

    PlanProto.WindowAggNode.Builder windowAggBuilder = PlanProto.WindowAggNode.newBuilder();
    windowAggBuilder.setChildSeq(childIds[0]);

    if (windowAgg.hasPartitionKeys()) {
      windowAggBuilder.addAllPartitionKeys(
          ProtoUtil.<CatalogProtos.ColumnProto>toProtoObjects(windowAgg.getPartitionKeys()));
    }

    if (windowAgg.hasAggFunctions()) {
      windowAggBuilder.addAllWindowFunctions(
          ProtoUtil.<PlanProto.EvalNodeTree>toProtoObjects(windowAgg.getWindowFunctions()));
    }
    windowAggBuilder.setDistinct(windowAgg.isDistinct());

    if (windowAgg.hasSortSpecs()) {
      windowAggBuilder.addAllSortSpecs(
          ProtoUtil.<CatalogProtos.SortSpecProto>toProtoObjects(windowAgg.getSortSpecs()));
    }
    if (windowAgg.hasTargets()) {
      windowAggBuilder.addAllTargets(
          ProtoUtil.<PlanProto.Target>toProtoObjects(windowAgg.getTargets()));
    }

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, windowAgg);
    nodeBuilder.setWindowAgg(windowAggBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return windowAgg;
  }

  @Override
  public LogicalNode visitSort(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                SortNode sort, Stack<LogicalNode> stack) throws PlanningException {
    super.visitSort(context, plan, block, sort, stack);

    int [] childIds = registerGetChildIds(context, sort);

    PlanProto.SortNode.Builder sortBuilder = PlanProto.SortNode.newBuilder();
    sortBuilder.setChildSeq(childIds[0]);
    for (int i = 0; i < sort.getSortKeys().length; i++) {
      sortBuilder.addSortSpecs(sort.getSortKeys()[i].getProto());
    }

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, sort);
    nodeBuilder.setSort(sortBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return sort;
  }

  @Override
  public LogicalNode visitHaving(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 HavingNode having, Stack<LogicalNode> stack) throws PlanningException {
    super.visitHaving(context, plan, block, having, stack);

    int [] childIds = registerGetChildIds(context, having);

    PlanProto.FilterNode.Builder filterBuilder = PlanProto.FilterNode.newBuilder();
    filterBuilder.setChildSeq(childIds[0]);
    filterBuilder.setQual(EvalNodeSerializer.serialize(having.getQual()));

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, having);
    nodeBuilder.setFilter(filterBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return having;
  }

  public LogicalNode visitGroupBy(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                  GroupbyNode node, Stack<LogicalNode> stack) throws PlanningException {
    super.visitGroupBy(context, plan, block, node, new Stack<LogicalNode>());

    PlanProto.LogicalNode.Builder nodeBuilder = buildGroupby(context, node);
    context.treeBuilder.addNodes(nodeBuilder);
    return node;
  }

  private PlanProto.LogicalNode.Builder buildGroupby(SerializeContext context, GroupbyNode node)
      throws PlanningException {
    int [] childIds = registerGetChildIds(context, node);

    PlanProto.GroupbyNode.Builder groupbyBuilder = PlanProto.GroupbyNode.newBuilder();
    groupbyBuilder.setChildSeq(childIds[0]);
    groupbyBuilder.setDistinct(node.isDistinct());

    if (node.groupingKeyNum() > 0) {
      groupbyBuilder.addAllGroupingKeys(
          ProtoUtil.<CatalogProtos.ColumnProto>toProtoObjects(node.getGroupingColumns()));
    }
    if (node.hasAggFunctions()) {
      groupbyBuilder.addAllAggFunctions(
          ProtoUtil.<PlanProto.EvalNodeTree>toProtoObjects(node.getAggFunctions()));
    }
    if (node.hasTargets()) {
      groupbyBuilder.addAllTargets(ProtoUtil.<PlanProto.Target>toProtoObjects(node.getTargets()));
    }

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, node);
    nodeBuilder.setGroupby(groupbyBuilder);

    return nodeBuilder;
  }

  public LogicalNode visitDistinctGroupby(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                          DistinctGroupbyNode node, Stack<LogicalNode> stack) throws PlanningException {
    super.visitDistinctGroupby(context, plan, block, node, new Stack<LogicalNode>());

    int [] childIds = registerGetChildIds(context, node);

    PlanProto.DistinctGroupbyNode.Builder distGroupbyBuilder = PlanProto.DistinctGroupbyNode.newBuilder();
    distGroupbyBuilder.setChildSeq(childIds[0]);
    if (node.getGroupbyPlan() != null) {
      distGroupbyBuilder.setGroupbyNode(buildGroupby(context, node.getGroupbyPlan()));
    }

    for (GroupbyNode subPlan : node.getSubPlans()) {
      distGroupbyBuilder.addSubPlans(buildGroupby(context, subPlan));
    }

    if (node.getGroupingColumns().length > 0) {
      distGroupbyBuilder.addAllGroupingKeys(
          ProtoUtil.<CatalogProtos.ColumnProto>toProtoObjects(node.getGroupingColumns()));
    }
    if (node.getAggFunctions().length > 0) {
      distGroupbyBuilder.addAllAggFunctions(
          ProtoUtil.<PlanProto.EvalNodeTree>toProtoObjects(node.getAggFunctions()));
    }
    if (node.hasTargets()) {
      distGroupbyBuilder.addAllTargets(ProtoUtil.<PlanProto.Target>toProtoObjects(node.getTargets()));
    }
    for (int cid : node.getResultColumnIds()) {
      distGroupbyBuilder.addResultId(cid);
    }

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, node);
    nodeBuilder.setDistinctGroupby(distGroupbyBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return node;
  }

  @Override
  public LogicalNode visitFilter(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 SelectionNode filter, Stack<LogicalNode> stack) throws PlanningException {
    super.visitFilter(context, plan, block, filter, stack);

    int [] childIds = registerGetChildIds(context, filter);

    PlanProto.FilterNode.Builder filterBuilder = PlanProto.FilterNode.newBuilder();
    filterBuilder.setChildSeq(childIds[0]);
    filterBuilder.setQual(EvalNodeSerializer.serialize(filter.getQual()));

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, filter);
    nodeBuilder.setFilter(filterBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return filter;
  }

  public LogicalNode visitJoin(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, JoinNode join,
                          Stack<LogicalNode> stack) throws PlanningException {
    super.visitJoin(context, plan, block, join, stack);

    int [] childIds = registerGetChildIds(context, join);

    // building itself
    PlanProto.JoinNode.Builder joinBuilder = PlanProto.JoinNode.newBuilder();
    joinBuilder.setJoinType(convertJoinType(join.getJoinType()));
    joinBuilder.setLeftChildSeq(childIds[0]);
    joinBuilder.setRightChilSeq(childIds[1]);
    if (join.hasJoinQual()) {
      joinBuilder.setJoinQual(EvalNodeSerializer.serialize(join.getJoinQual()));
    }

    if (join.hasTargets()) {
      joinBuilder.setExistsTargets(true);
      joinBuilder.addAllTargets(ProtoUtil.<PlanProto.Target>toProtoObjects(join.getTargets()));
    } else {
      joinBuilder.setExistsTargets(false);
    }

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, join);
    nodeBuilder.setJoin(joinBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return join;
  }

  @Override
  public LogicalNode visitUnion(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, UnionNode node,
                           Stack<LogicalNode> stack) throws PlanningException {
    super.visitUnion(context, plan, block, node, stack);

    int [] childIds = registerGetChildIds(context, node);

    PlanProto.UnionNode.Builder unionBuilder = PlanProto.UnionNode.newBuilder();
    unionBuilder.setAll(true);
    unionBuilder.setLeftChildSeq(childIds[0]);
    unionBuilder.setRightChildSeq(childIds[1]);

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, node);
    nodeBuilder.setUnion(unionBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return node;
  }

  @Override
  public LogicalNode visitScan(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                               ScanNode scan, Stack<LogicalNode> stack) throws PlanningException {

    PlanProto.ScanNode.Builder scanBuilder = buildScanNode(scan);

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, scan);
    nodeBuilder.setScan(scanBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return scan;
  }

  public PlanProto.ScanNode.Builder buildScanNode(ScanNode scan) {
    PlanProto.ScanNode.Builder scanBuilder = PlanProto.ScanNode.newBuilder();
    scanBuilder.setTable(scan.getTableDesc().getProto());
    if (scan.hasAlias()) {
      scanBuilder.setAlias(scan.getAlias());
    }

    if (scan.hasTargets()) {
      scanBuilder.setExistTargets(true);
      scanBuilder.addAllTargets(ProtoUtil.<PlanProto.Target>toProtoObjects(scan.getTargets()));
    } else {
      scanBuilder.setExistTargets(false);
    }

    if (scan.hasQual()) {
      scanBuilder.setQual(EvalNodeSerializer.serialize(scan.getQual()));
    }

    scanBuilder.setBroadcast(scan.isBroadcastTable());
    return scanBuilder;
  }

  @Override
  public LogicalNode visitPartitionedTableScan(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                          PartitionedTableScanNode node, Stack<LogicalNode> stack)
      throws PlanningException {

    PlanProto.ScanNode.Builder scanBuilder = buildScanNode(node);

    PlanProto.PartitionScanSpec.Builder partitionScan = PlanProto.PartitionScanSpec.newBuilder();
    List<String> pathStrs = TUtil.newList();
    if (node.getInputPaths() != null) {
      for (Path p : node.getInputPaths()) {
        pathStrs.add(p.toString());
      }
      partitionScan.addAllPaths(pathStrs);
    }

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, node);
    nodeBuilder.setScan(scanBuilder);
    nodeBuilder.setPartitionScan(partitionScan);
    context.treeBuilder.addNodes(nodeBuilder);

    return node;
  }

  public LogicalNode visitTableSubQuery(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                   TableSubQueryNode node, Stack<LogicalNode> stack) throws PlanningException {
    super.visitTableSubQuery(context, plan, block, node, stack);

    int [] childIds = registerGetChildIds(context, node);

    PlanProto.TableSubQueryNode.Builder builder = PlanProto.TableSubQueryNode.newBuilder();
    builder.setChildSeq(childIds[0]);

    builder.setTableName(node.getTableName());

    if (node.hasTargets()) {
      builder.addAllTargets(ProtoUtil.<PlanProto.Target>toProtoObjects(node.getTargets()));
    }

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, node);
    nodeBuilder.setTableSubQuery(builder);
    context.treeBuilder.addNodes(nodeBuilder);

    return node;
  }

  public LogicalNode visitCreateTable(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                      CreateTableNode node, Stack<LogicalNode> stack) throws PlanningException {
    super.visitCreateTable(context, plan, block, node, stack);

    int [] childIds = registerGetChildIds(context, node);

    PlanProto.PersistentStoreNode.Builder persistentStoreBuilder = buildPersistentStoreBuilder(node, childIds);
    PlanProto.StoreTableNodeSpec.Builder storeTableBuilder = buildStoreTableNodeSpec(node);

    PlanProto.CreateTableNodeSpec.Builder createTableBuilder = PlanProto.CreateTableNodeSpec.newBuilder();
    createTableBuilder.setSchema(node.getTableSchema().getProto());
    createTableBuilder.setExternal(node.isExternal());
    if (node.isExternal() && node.hasPath()) {
      createTableBuilder.setPath(node.getPath().toString());
    }
    createTableBuilder.setIfNotExists(node.isIfNotExists());

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, node);
    nodeBuilder.setPersistentStore(persistentStoreBuilder);
    nodeBuilder.setStoreTable(storeTableBuilder);
    nodeBuilder.setCreateTable(createTableBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return node;
  }

  @Override
  public LogicalNode visitDropTable(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                    DropTableNode node, Stack<LogicalNode> stack) {
    PlanProto.DropTableNode.Builder dropTableBuilder = PlanProto.DropTableNode.newBuilder();
    dropTableBuilder.setTableName(node.getTableName());
    dropTableBuilder.setIfExists(node.isIfExists());
    dropTableBuilder.setPurge(node.isPurge());

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, node);
    nodeBuilder.setDropTable(dropTableBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return node;
  }

  @Override
  public LogicalNode visitAlterTablespace(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                     AlterTablespaceNode node, Stack<LogicalNode> stack) throws PlanningException {
    PlanProto.AlterTablespaceNode.Builder alterTablespaceBuilder = PlanProto.AlterTablespaceNode.newBuilder();
    alterTablespaceBuilder.setTableSpaceName(node.getTablespaceName());

    switch (node.getSetType()) {
    case LOCATION:
      alterTablespaceBuilder.setSetType(PlanProto.AlterTablespaceNode.Type.LOCATION);
      alterTablespaceBuilder.setSetLocation(SetLocation.newBuilder().setLocation(node.getLocation()));
      break;

    default:
      throw new UnimplementedException("Unknown SET type in ALTER TABLESPACE: " + node.getSetType().name());
    }

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, node);
    nodeBuilder.setAlterTablespace(alterTablespaceBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return node;
  }

  @Override
  public LogicalNode visitAlterTable(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                     AlterTableNode node, Stack<LogicalNode> stack) {
    PlanProto.AlterTableNode.Builder alterTableBuilder = PlanProto.AlterTableNode.newBuilder();
    alterTableBuilder.setTableName(node.getTableName());

    switch (node.getAlterTableOpType()) {
    case RENAME_TABLE:
      alterTableBuilder.setSetType(PlanProto.AlterTableNode.Type.RENAME_TABLE);
      alterTableBuilder.setRenameTable(RenameTable.newBuilder().setNewName(node.getNewTableName()));
      break;
    case ADD_COLUMN:
      alterTableBuilder.setSetType(PlanProto.AlterTableNode.Type.ADD_COLUMN);
      alterTableBuilder.setAddColumn(AddColumn.newBuilder().setAddColumn(node.getAddNewColumn().getProto()));
      break;
    case RENAME_COLUMN:
      alterTableBuilder.setSetType(PlanProto.AlterTableNode.Type.RENAME_COLUMN);
      alterTableBuilder.setRenameColumn(RenameColumn.newBuilder()
          .setOldName(node.getColumnName())
          .setNewName(node.getNewColumnName()));
      break;
    case SET_PROPERTY:
      alterTableBuilder.setSetType(PlanProto.AlterTableNode.Type.SET_PROPERTY);
      alterTableBuilder.setProperties(node.getProperties().getProto());
      break;
    default:
      throw new UnimplementedException("Unknown SET type in ALTER TABLE: " + node.getAlterTableOpType().name());
    }

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, node);
    nodeBuilder.setAlterTable(alterTableBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return node;
  }

  @Override
  public LogicalNode visitTruncateTable(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                   TruncateTableNode node, Stack<LogicalNode> stack) throws PlanningException {
    PlanProto.TruncateTableNode.Builder truncateTableBuilder = PlanProto.TruncateTableNode.newBuilder();
    truncateTableBuilder.addAllTableNames(node.getTableNames());

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, node);
    nodeBuilder.setTruncateTableNode(truncateTableBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return node;
  }

  public LogicalNode visitInsert(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 InsertNode node, Stack<LogicalNode> stack) throws PlanningException {
    super.visitInsert(context, plan, block, node, stack);

    int [] childIds = registerGetChildIds(context, node);

    PlanProto.PersistentStoreNode.Builder persistentStoreBuilder = buildPersistentStoreBuilder(node, childIds);
    PlanProto.StoreTableNodeSpec.Builder storeTableBuilder = buildStoreTableNodeSpec(node);

    PlanProto.InsertNodeSpec.Builder insertNodeSpec = PlanProto.InsertNodeSpec.newBuilder();
    insertNodeSpec.setOverwrite(node.isOverwrite());
    insertNodeSpec.setTableSchema(node.getTableSchema().getProto());
    if (node.hasProjectedSchema()) {
      insertNodeSpec.setProjectedSchema(node.getProjectedSchema().getProto());
    }
    if (node.hasTargetSchema()) {
      insertNodeSpec.setTargetSchema(node.getTargetSchema().getProto());
    }
    if (node.hasPath()) {
      insertNodeSpec.setPath(node.getPath().toString());
    }

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, node);
    nodeBuilder.setPersistentStore(persistentStoreBuilder);
    nodeBuilder.setStoreTable(storeTableBuilder);
    nodeBuilder.setInsert(insertNodeSpec);
    context.treeBuilder.addNodes(nodeBuilder);

    return node;
  }

  private static PlanProto.PersistentStoreNode.Builder buildPersistentStoreBuilder(PersistentStoreNode node,
                                                                                   int [] childIds) {
    PlanProto.PersistentStoreNode.Builder persistentStoreBuilder = PlanProto.PersistentStoreNode.newBuilder();
    persistentStoreBuilder.setChildSeq(childIds[0]);
    persistentStoreBuilder.setStorageType(node.getStorageType());
    if (node.hasOptions()) {
      persistentStoreBuilder.setTableProperties(node.getOptions().getProto());
    }
    return persistentStoreBuilder;
  }

  private static PlanProto.StoreTableNodeSpec.Builder buildStoreTableNodeSpec(StoreTableNode node) {
    PlanProto.StoreTableNodeSpec.Builder storeTableBuilder = PlanProto.StoreTableNodeSpec.newBuilder();
    if (node.hasPartition()) {
      storeTableBuilder.setPartitionMethod(node.getPartitionMethod().getProto());
    }
    if (node.hasTableName()) { // It will be false if node is for INSERT INTO LOCATION '...'
      storeTableBuilder.setTableName(node.getTableName());
    }
    return storeTableBuilder;
  }

  @Override
  public LogicalNode visitCreateDatabase(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                    CreateDatabaseNode node, Stack<LogicalNode> stack) throws PlanningException {
    PlanProto.CreateDatabaseNode.Builder createDatabaseBuilder = PlanProto.CreateDatabaseNode.newBuilder();
    createDatabaseBuilder.setDbName(node.getDatabaseName());
    createDatabaseBuilder.setIfNotExists(node.isIfNotExists());

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, node);
    nodeBuilder.setCreateDatabase(createDatabaseBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return node;
  }

  @Override
  public LogicalNode visitDropDatabase(SerializeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                       DropDatabaseNode node, Stack<LogicalNode> stack) throws PlanningException {
    PlanProto.DropDatabaseNode.Builder dropDatabaseBuilder = PlanProto.DropDatabaseNode.newBuilder();
    dropDatabaseBuilder.setDbName(node.getDatabaseName());
    dropDatabaseBuilder.setIfExists(node.isIfExists());

    PlanProto.LogicalNode.Builder nodeBuilder = createNodeBuilder(context, node);
    nodeBuilder.setDropDatabase(dropDatabaseBuilder);
    context.treeBuilder.addNodes(nodeBuilder);

    return node;
  }

  public static PlanProto.NodeType convertType(NodeType type) {
    return PlanProto.NodeType.valueOf(type.name());
  }

  public static PlanProto.JoinType convertJoinType(JoinType type) {
    switch (type) {
    case CROSS:
      return PlanProto.JoinType.CROSS_JOIN;
    case INNER:
      return PlanProto.JoinType.INNER_JOIN;
    case LEFT_OUTER:
      return PlanProto.JoinType.LEFT_OUTER_JOIN;
    case RIGHT_OUTER:
      return PlanProto.JoinType.RIGHT_OUTER_JOIN;
    case FULL_OUTER:
      return PlanProto.JoinType.FULL_OUTER_JOIN;
    case LEFT_SEMI:
      return PlanProto.JoinType.LEFT_SEMI_JOIN;
    case RIGHT_SEMI:
      return PlanProto.JoinType.RIGHT_SEMI_JOIN;
    case LEFT_ANTI:
      return PlanProto.JoinType.LEFT_ANTI_JOIN;
    case RIGHT_ANTI:
      return PlanProto.JoinType.RIGHT_ANTI_JOIN;
    case UNION:
      return PlanProto.JoinType.UNION_JOIN;
    default:
      throw new RuntimeException("Unknown JoinType: " + type.name());
    }
  }

  public static PlanProto.Target convertTarget(Target target) {
    PlanProto.Target.Builder targetBuilder = PlanProto.Target.newBuilder();
    targetBuilder.setExpr(EvalNodeSerializer.serialize(target.getEvalTree()));
    if (target.hasAlias()) {
      targetBuilder.setAlias(target.getAlias());
    }
    return targetBuilder.build();
  }

  private int [] registerGetChildIds(SerializeContext context, LogicalNode node) {
    int [] childIds = new int[node.childNum()];
    for (int i = 0; i < node.childNum(); i++) {
      if (context.idMap.containsKey(node.getChild(i))) {
        childIds[i] = context.idMap.get(node.getChild(i));
      } else {
        childIds[i] = context.seqId++;
      }
    }
    return childIds;
  }
}
