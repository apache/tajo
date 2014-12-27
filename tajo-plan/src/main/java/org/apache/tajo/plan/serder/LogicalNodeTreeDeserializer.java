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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.exception.UnimplementedException;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.AggregationFunctionCallEval;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.expr.FieldEval;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.TUtil;

import java.util.*;

public class LogicalNodeTreeDeserializer {
  private static final LogicalNodeTreeDeserializer instance;

  static {
    instance = new LogicalNodeTreeDeserializer();
  }

  public static LogicalNode deserialize(OverridableConf context, PlanProto.LogicalNodeTree tree) {
    Map<Integer, LogicalNode> nodeMap = Maps.newHashMap();

    // sort serialized logical nodes in an ascending order of their sids
    List<PlanProto.LogicalNode> nodeList = Lists.newArrayList(tree.getNodesList());
    Collections.sort(nodeList, new Comparator<PlanProto.LogicalNode>() {
      @Override
      public int compare(PlanProto.LogicalNode o1, PlanProto.LogicalNode o2) {
        return o1.getSid() - o2.getSid();
      }
    });

    LogicalNode current = null;

    // The sorted order is the same of a postfix traverse order.
    // So, it sequentially transforms each serialized node into a LogicalNode instance in a postfix order of
    // the original logical node tree.

    Iterator<PlanProto.LogicalNode> it = nodeList.iterator();
    while (it.hasNext()) {
      PlanProto.LogicalNode protoNode = it.next();

      switch (protoNode.getType()) {
      case ROOT:
        current = convertRoot(nodeMap, protoNode);
        break;
      case SET_SESSION:
        current = convertSetSession(protoNode);
        break;
      case EXPRS:
        current = convertEvalExpr(context, protoNode);
        break;
      case PROJECTION:
        current = convertProjection(context, nodeMap, protoNode);
        break;
      case LIMIT:
        current = convertLimit(nodeMap, protoNode);
        break;
      case SORT:
        current = convertSort(nodeMap, protoNode);
        break;
      case HAVING:
        current = convertHaving(context, nodeMap, protoNode);
        break;
      case GROUP_BY:
        current = convertGroupby(context, nodeMap, protoNode);
        break;
      case DISTINCT_GROUP_BY:
        current = convertDistinctGroupby(context, nodeMap, protoNode);
        break;
      case SELECTION:
        current = convertFilter(context, nodeMap, protoNode);
        break;
      case JOIN:
        current = convertJoin(context, nodeMap, protoNode);
        break;
      case TABLE_SUBQUERY:
        current = convertTableSubQuery(context, nodeMap, protoNode);
        break;
      case UNION:
        current = convertUnion(nodeMap, protoNode);
        break;
      case SCAN:
        current = convertScan(context, protoNode);
        break;

      case CREATE_TABLE:
        current = convertCreateTable(nodeMap, protoNode);
        break;
      case INSERT:
        current = convertInsert(nodeMap, protoNode);
        break;
      case DROP_TABLE:
        current = convertDropTable(protoNode);
        break;

      case CREATE_DATABASE:
        current = convertCreateDatabase(protoNode);
        break;
      case DROP_DATABASE:
        current = convertDropDatabase(protoNode);
        break;

      case ALTER_TABLESPACE:
        current = convertAlterTablespace(protoNode);
        break;
      case ALTER_TABLE:
        current = convertAlterTable(protoNode);
        break;
      case TRUNCATE_TABLE:
        current = convertTruncateTable(protoNode);
        break;

      default:
        throw new RuntimeException("Unknown NodeType: " + protoNode.getType().name());
      }

      nodeMap.put(protoNode.getSid(), current);
    }

    return current;
  }

  public static LogicalRootNode convertRoot(Map<Integer, LogicalNode> nodeMap,
                                            PlanProto.LogicalNode protoNode) {
    PlanProto.RootNode rootProto = protoNode.getRoot();

    LogicalRootNode root = new LogicalRootNode(protoNode.getPid());
    root.setChild(nodeMap.get(rootProto.getChildId()));
    if (protoNode.hasInSchema()) {
      root.setInSchema(convertSchema(protoNode.getInSchema()));
    }
    if (protoNode.hasOutSchema()) {
      root.setOutSchema(convertSchema(protoNode.getOutSchema()));
    }

    return root;
  }

  public static SetSessionNode convertSetSession(PlanProto.LogicalNode protoNode) {
    PlanProto.SetSessionNode setSessionProto = protoNode.getSetSession();

    SetSessionNode setSession = new SetSessionNode(protoNode.getPid());
    setSession.init(setSessionProto.getName(), setSessionProto.hasValue() ? setSessionProto.getValue() : null);

    return setSession;
  }

  public static EvalExprNode convertEvalExpr(OverridableConf context, PlanProto.LogicalNode protoNode) {
    PlanProto.EvalExprNode evalExprProto = protoNode.getExprEval();

    EvalExprNode evalExpr = new EvalExprNode(protoNode.getPid());
    evalExpr.setInSchema(convertSchema(protoNode.getInSchema()));
    evalExpr.setTargets(convertTargets(context, evalExprProto.getTargetsList()));

    return evalExpr;
  }

  public static ProjectionNode convertProjection(OverridableConf context, Map<Integer, LogicalNode> nodeMap,
                                                 PlanProto.LogicalNode protoNode) {
    PlanProto.ProjectionNode projectionProto = protoNode.getProjection();

    ProjectionNode projectionNode = new ProjectionNode(protoNode.getPid());
    projectionNode.init(projectionProto.getDistinct(), convertTargets(context, projectionProto.getTargetsList()));
    projectionNode.setChild(nodeMap.get(projectionProto.getChildId()));
    projectionNode.setInSchema(convertSchema(protoNode.getInSchema()));
    projectionNode.setOutSchema(convertSchema(protoNode.getOutSchema()));

    return projectionNode;
  }

  public static LimitNode convertLimit(Map<Integer, LogicalNode> nodeMap, PlanProto.LogicalNode protoNode) {
    PlanProto.LimitNode limitProto = protoNode.getLimit();

    LimitNode limitNode = new LimitNode(protoNode.getPid());
    limitNode.setChild(nodeMap.get(limitProto.getChildId()));
    limitNode.setInSchema(convertSchema(protoNode.getInSchema()));
    limitNode.setOutSchema(convertSchema(protoNode.getOutSchema()));
    limitNode.setFetchFirst(limitProto.getFetchFirstNum());

    return limitNode;
  }

  public static SortNode convertSort(Map<Integer, LogicalNode> nodeMap, PlanProto.LogicalNode protoNode) {
    PlanProto.SortNode sortProto = protoNode.getSort();

    SortNode sortNode = new SortNode(protoNode.getPid());
    sortNode.setChild(nodeMap.get(sortProto.getChildId()));
    sortNode.setInSchema(convertSchema(protoNode.getInSchema()));
    sortNode.setOutSchema(convertSchema(protoNode.getOutSchema()));
    sortNode.setSortSpecs(convertSortSpecs(sortProto.getSortSpecsList()));

    return sortNode;
  }

  public static HavingNode convertHaving(OverridableConf context, Map<Integer, LogicalNode> nodeMap,
                                         PlanProto.LogicalNode protoNode) {
    PlanProto.FilterNode havingProto = protoNode.getFilter();

    HavingNode having = new HavingNode(protoNode.getPid());
    having.setChild(nodeMap.get(havingProto.getChildId()));
    having.setQual(EvalTreeProtoDeserializer.deserialize(context, havingProto.getQual()));
    having.setInSchema(convertSchema(protoNode.getInSchema()));
    having.setOutSchema(convertSchema(protoNode.getOutSchema()));

    return having;
  }

  public static GroupbyNode convertGroupby(OverridableConf context, Map<Integer, LogicalNode> nodeMap,
                                           PlanProto.LogicalNode protoNode) {
    PlanProto.GroupbyNode groupbyProto = protoNode.getGroupby();

    GroupbyNode groupby = new GroupbyNode(protoNode.getPid());
    groupby.setChild(nodeMap.get(groupbyProto.getChildId()));

    if (groupbyProto.getGroupingKeysCount() > 0) {
      groupby.setGroupingColumns(convertColumns(groupbyProto.getGroupingKeysList()));
    }
    if (groupbyProto.getAggFunctionsCount() > 0) {
      groupby.setAggFunctions(convertAggFuncCallEvals(context, groupbyProto.getAggFunctionsList()));
    }
    if (groupbyProto.getTargetsCount() > 0) {
      groupby.setTargets(convertTargets(context, groupbyProto.getTargetsList()));
    }

    groupby.setInSchema(convertSchema(protoNode.getInSchema()));
    groupby.setOutSchema(convertSchema(protoNode.getOutSchema()));

    return groupby;
  }

  public static DistinctGroupbyNode convertDistinctGroupby(OverridableConf context, Map<Integer, LogicalNode> nodeMap,
                                           PlanProto.LogicalNode protoNode) {
    PlanProto.DistinctGroupbyNode distinctGroupbyProto = protoNode.getDistinctGroupby();

    DistinctGroupbyNode distinctGroupby = new DistinctGroupbyNode(protoNode.getPid());
    distinctGroupby.setChild(nodeMap.get(distinctGroupbyProto.getChildId()));

    if (distinctGroupbyProto.hasGroupbyNode()) {
      distinctGroupby.setGroupbyPlan(convertGroupby(context, nodeMap, distinctGroupbyProto.getGroupbyNode()));
    }

    if (distinctGroupbyProto.getSubPlansCount() > 0) {
      List<GroupbyNode> subPlans = TUtil.newList();
      for (int i = 0; i < distinctGroupbyProto.getSubPlansCount(); i++) {
        subPlans.add(convertGroupby(context, nodeMap, distinctGroupbyProto.getSubPlans(i)));
      }
      distinctGroupby.setSubPlans(subPlans);
    }

    if (distinctGroupbyProto.getGroupingKeysCount() > 0) {
      distinctGroupby.setGroupingColumns(convertColumns(distinctGroupbyProto.getGroupingKeysList()));
    }
    if (distinctGroupbyProto.getAggFunctionsCount() > 0) {
      distinctGroupby.setAggFunctions(convertAggFuncCallEvals(context, distinctGroupbyProto.getAggFunctionsList()));
    }
    if (distinctGroupbyProto.getTargetsCount() > 0) {
      distinctGroupby.setTargets(convertTargets(context, distinctGroupbyProto.getTargetsList()));
    }
    int [] resultColumnIds = new int[distinctGroupbyProto.getResultIdCount()];
    for (int i = 0; i < distinctGroupbyProto.getResultIdCount(); i++) {
      resultColumnIds[i] = distinctGroupbyProto.getResultId(i);
    }
    distinctGroupby.setResultColumnIds(resultColumnIds);

    // TODO - in distinct groupby, output and target are not matched to each other. It does not follow the convention.
    distinctGroupby.setInSchema(convertSchema(protoNode.getInSchema()));
    distinctGroupby.setOutSchema(convertSchema(protoNode.getOutSchema()));

    return distinctGroupby;
  }

  public static JoinNode convertJoin(OverridableConf context, Map<Integer, LogicalNode> nodeMap,
                                     PlanProto.LogicalNode protoNode) {
    PlanProto.JoinNode joinProto = protoNode.getJoin();

    JoinNode join = new JoinNode(protoNode.getPid());
    join.setLeftChild(nodeMap.get(joinProto.getLeftChildId()));
    join.setRightChild(nodeMap.get(joinProto.getRightChildId()));
    join.setJoinType(convertJoinType(joinProto.getJoinType()));
    join.setInSchema(convertSchema(protoNode.getInSchema()));
    join.setOutSchema(convertSchema(protoNode.getOutSchema()));
    if (joinProto.hasJoinQual()) {
      join.setJoinQual(EvalTreeProtoDeserializer.deserialize(context, joinProto.getJoinQual()));
    }
    if (joinProto.getTargetsCount() > 0) {
      join.setTargets(convertTargets(context, joinProto.getTargetsList()));
    }

    return join;
  }

  public static SelectionNode convertFilter(OverridableConf context, Map<Integer, LogicalNode> nodeMap,
                                            PlanProto.LogicalNode protoNode) {
    PlanProto.FilterNode filterProto = protoNode.getFilter();

    SelectionNode selection = new SelectionNode(protoNode.getPid());
    selection.setInSchema(convertSchema(protoNode.getInSchema()));
    selection.setOutSchema(convertSchema(protoNode.getOutSchema()));
    selection.setChild(nodeMap.get(filterProto.getChildId()));
    selection.setQual(EvalTreeProtoDeserializer.deserialize(context, filterProto.getQual()));

    return selection;
  }

  public static UnionNode convertUnion(Map<Integer, LogicalNode> nodeMap, PlanProto.LogicalNode protoNode) {
    PlanProto.UnionNode unionProto = protoNode.getUnion();

    UnionNode union = new UnionNode(protoNode.getPid());
    union.setInSchema(convertSchema(protoNode.getInSchema()));
    union.setOutSchema(convertSchema(protoNode.getOutSchema()));
    union.setLeftChild(nodeMap.get(unionProto.getLeftChildId()));
    union.setRightChild(nodeMap.get(unionProto.getRightChildId()));

    return union;
  }

  public static ScanNode convertScan(OverridableConf context, PlanProto.LogicalNode protoNode) {
    ScanNode scan = new ScanNode(protoNode.getPid());

    PlanProto.ScanNode scanProto = protoNode.getScan();
    if (scanProto.hasAlias()) {
      scan.init(new TableDesc(scanProto.getTable()), scanProto.getAlias());
    } else {
      scan.init(new TableDesc(scanProto.getTable()));
    }

    if (scanProto.getExistTargets()) {
      scan.setTargets(convertTargets(context, scanProto.getTargetsList()));
    }

    if (scanProto.hasQual()) {
      scan.setQual(EvalTreeProtoDeserializer.deserialize(context, scanProto.getQual()));
    }

    scan.setInSchema(convertSchema(protoNode.getInSchema()));
    scan.setOutSchema(convertSchema(protoNode.getOutSchema()));

    return scan;
  }

  public static TableSubQueryNode convertTableSubQuery(OverridableConf context,
                                                                 Map<Integer, LogicalNode> nodeMap,
                                                                 PlanProto.LogicalNode protoNode) {
    PlanProto.TableSubQueryNode proto = protoNode.getTableSubQuery();

    TableSubQueryNode tableSubQuery = new TableSubQueryNode(protoNode.getPid());
    tableSubQuery.init(proto.getTableName(), nodeMap.get(proto.getChildId()));
    tableSubQuery.setInSchema(convertSchema(protoNode.getInSchema()));
    if (proto.getTargetsCount() > 0) {
      tableSubQuery.setTargets(convertTargets(context, proto.getTargetsList()));
    }

    return tableSubQuery;
  }

  public static CreateTableNode convertCreateTable(Map<Integer, LogicalNode> nodeMap,
                                            PlanProto.LogicalNode protoNode) {
    PlanProto.PersistentStoreNode persistentStoreProto = protoNode.getPersistentStore();
    PlanProto.StoreTableNodeSpec storeTableNodeSpec = protoNode.getStoreTable();
    PlanProto.CreateTableNodeSpec createTableNodeSpec = protoNode.getCreateTable();

    CreateTableNode createTable = new CreateTableNode(protoNode.getPid());
    if (protoNode.hasInSchema()) {
      createTable.setInSchema(convertSchema(protoNode.getInSchema()));
    }
    if (protoNode.hasOutSchema()) {
      createTable.setOutSchema(convertSchema(protoNode.getOutSchema()));
    }
    createTable.setChild(nodeMap.get(persistentStoreProto.getChildId()));
    createTable.setStorageType(persistentStoreProto.getStorageType());
    createTable.setOptions(new KeyValueSet(persistentStoreProto.getTableProperties()));

    createTable.setTableName(storeTableNodeSpec.getTableName());
    if (storeTableNodeSpec.hasPartitionMethod()) {
      createTable.setPartitionMethod(new PartitionMethodDesc(storeTableNodeSpec.getPartitionMethod()));
    }

    createTable.setTableSchema(convertSchema(createTableNodeSpec.getSchema()));
    createTable.setExternal(createTableNodeSpec.getExternal());
    if (createTableNodeSpec.getExternal() && createTableNodeSpec.hasPath()) {
      createTable.setPath(new Path(createTableNodeSpec.getPath()));
    }
    createTable.setIfNotExists(createTableNodeSpec.getIfNotExists());

    return createTable;
  }

  public static InsertNode convertInsert(Map<Integer, LogicalNode> nodeMap,
                                                   PlanProto.LogicalNode protoNode) {
    PlanProto.PersistentStoreNode persistentStoreProto = protoNode.getPersistentStore();
    PlanProto.StoreTableNodeSpec storeTableNodeSpec = protoNode.getStoreTable();
    PlanProto.InsertNodeSpec insertNodeSpec = protoNode.getInsert();

    InsertNode insertNode = new InsertNode(protoNode.getPid());
    if (protoNode.hasInSchema()) {
      insertNode.setInSchema(convertSchema(protoNode.getInSchema()));
    }
    if (protoNode.hasOutSchema()) {
      insertNode.setOutSchema(convertSchema(protoNode.getOutSchema()));
    }
    insertNode.setChild(nodeMap.get(persistentStoreProto.getChildId()));
    insertNode.setStorageType(persistentStoreProto.getStorageType());
    insertNode.setOptions(new KeyValueSet(persistentStoreProto.getTableProperties()));

    if (storeTableNodeSpec.hasTableName()) {
      insertNode.setTableName(storeTableNodeSpec.getTableName());
    }
    if (storeTableNodeSpec.hasPartitionMethod()) {
      insertNode.setPartitionMethod(new PartitionMethodDesc(storeTableNodeSpec.getPartitionMethod()));
    }

    insertNode.setOverwrite(insertNodeSpec.getOverwrite());
    insertNode.setTableSchema(convertSchema(insertNodeSpec.getTableSchema()));
    if (insertNodeSpec.hasTargetSchema()) {
      insertNode.setTargetSchema(convertSchema(insertNodeSpec.getTargetSchema()));
    }
    if (insertNodeSpec.hasProjectedSchema()) {
      insertNode.setProjectedSchema(convertSchema(insertNodeSpec.getProjectedSchema()));
    }
    if (insertNodeSpec.hasPath()) {
      insertNode.setPath(new Path(insertNodeSpec.getPath()));
    }

    return insertNode;
  }

  public static DropTableNode convertDropTable(PlanProto.LogicalNode protoNode) {
    DropTableNode dropTable = new DropTableNode(protoNode.getPid());

    PlanProto.DropTableNode dropTableProto = protoNode.getDropTable();
    dropTable.init(dropTableProto.getTableName(), dropTableProto.getIfExists(), dropTableProto.getPurge());

    return dropTable;
  }

  public static CreateDatabaseNode convertCreateDatabase(PlanProto.LogicalNode protoNode) {
    CreateDatabaseNode createDatabase = new CreateDatabaseNode(protoNode.getPid());

    PlanProto.CreateDatabaseNode createDatabaseProto = protoNode.getCreateDatabase();
    createDatabase.init(createDatabaseProto.getDbName(), createDatabaseProto.getIfNotExists());

    return createDatabase;
  }

  public static DropDatabaseNode convertDropDatabase(PlanProto.LogicalNode protoNode) {
    DropDatabaseNode dropDatabase = new DropDatabaseNode(protoNode.getPid());

    PlanProto.DropDatabaseNode dropDatabaseProto = protoNode.getDropDatabase();
    dropDatabase.init(dropDatabaseProto.getDbName(), dropDatabaseProto.getIfExists());

    return dropDatabase;
  }

  public static AlterTablespaceNode convertAlterTablespace(PlanProto.LogicalNode protoNode) {
    AlterTablespaceNode alterTablespace = new AlterTablespaceNode(protoNode.getPid());

    PlanProto.AlterTablespaceNode alterTablespaceProto = protoNode.getAlterTablespace();
    alterTablespace.setTablespaceName(alterTablespaceProto.getTableSpaceName());

    switch (alterTablespaceProto.getSetType()) {
    case LOCATION:
      alterTablespace.setLocation(alterTablespaceProto.getSetLocation().getLocation());
      break;
    default:
      throw new UnimplementedException("Unknown SET type in ALTER TABLE: " + alterTablespaceProto.getSetType().name());
    }

    return alterTablespace;
  }

  public static AlterTableNode convertAlterTable(PlanProto.LogicalNode protoNode) {
    AlterTableNode alterTable = new AlterTableNode(protoNode.getPid());

    PlanProto.AlterTableNode alterTableProto = protoNode.getAlterTable();
    alterTable.setTableName(alterTableProto.getTableName());

    switch (alterTableProto.getSetType()) {
    case RENAME_TABLE:
      alterTable.setNewTableName(alterTableProto.getRenameTable().getNewName());
      break;
    case ADD_COLUMN:
      alterTable.setAddNewColumn(new Column(alterTableProto.getAddColumn().getAddColumn()));
      break;
    case RENAME_COLUMN:
      alterTable.setColumnName(alterTableProto.getRenameColumn().getOldName());
      alterTable.setNewColumnName(alterTableProto.getRenameColumn().getNewName());
      break;
    default:
      throw new UnimplementedException("Unknown SET type in ALTER TABLE: " + alterTableProto.getSetType().name());
    }

    return alterTable;
  }

  public static TruncateTableNode convertTruncateTable(PlanProto.LogicalNode protoNode) {
    TruncateTableNode truncateTable = new TruncateTableNode(protoNode.getPid());

    PlanProto.TruncateTableNode truncateTableProto = protoNode.getTruncateTableNode();
    truncateTable.setTableNames(truncateTableProto.getTableNamesList());

    return truncateTable;
  }

  public static Schema convertSchema(CatalogProtos.SchemaProto proto) {
    return new Schema(proto);
  }

  public static <T extends EvalNode> T [] convertEvalNodes(OverridableConf context,
                                                           List<PlanProto.EvalTree> evalTrees) {
    EvalNode [] evalNodes = new EvalNode[evalTrees.size()];
    for (int i = 0; i < evalNodes.length; i++) {
      evalNodes[i] = EvalTreeProtoDeserializer.deserialize(context, evalTrees.get(i));
    }
    return (T[]) evalNodes;
  }

  public static AggregationFunctionCallEval [] convertAggFuncCallEvals(OverridableConf context,
                                                                       List<PlanProto.EvalTree> evalTrees) {
    AggregationFunctionCallEval [] aggFuncs = new AggregationFunctionCallEval[evalTrees.size()];
    for (int i = 0; i < aggFuncs.length; i++) {
      aggFuncs[i] = (AggregationFunctionCallEval) EvalTreeProtoDeserializer.deserialize(context, evalTrees.get(i));
    }
    return aggFuncs;
  }

  public static Column[] convertColumns(List<CatalogProtos.ColumnProto> columnProtos) {
    Column [] columns = new Column[columnProtos.size()];
    for (int i = 0; i < columns.length; i++) {
      columns[i] = new Column(columnProtos.get(i));
    }
    return columns;
  }

  public static Target[] convertTargets(OverridableConf context, List<PlanProto.Target> targetsProto) {
    Target [] targets = new Target[targetsProto.size()];
    for (int i = 0; i < targets.length; i++) {
      PlanProto.Target targetProto = targetsProto.get(i);
      EvalNode evalNode = EvalTreeProtoDeserializer.deserialize(context, targetProto.getExpr());
      if (targetProto.hasAlias()) {
        targets[i] = new Target(evalNode, targetProto.getAlias());
      } else {
        targets[i] = new Target((FieldEval) evalNode);
      }
    }
    return targets;
  }

  public static SortSpec[] convertSortSpecs(List<CatalogProtos.SortSpecProto> sortSpecProtos) {
    SortSpec[] sortSpecs = new SortSpec[sortSpecProtos.size()];
    int i = 0;
    for (CatalogProtos.SortSpecProto proto : sortSpecProtos) {
      sortSpecs[i++] = new SortSpec(proto);
    }
    return sortSpecs;
  }

  public static JoinType convertJoinType(PlanProto.JoinType type) {
    switch (type) {
    case CROSS_JOIN:
      return JoinType.CROSS;
    case INNER_JOIN:
      return JoinType.INNER;
    case LEFT_OUTER_JOIN:
      return JoinType.LEFT_OUTER;
    case RIGHT_OUTER_JOIN:
      return JoinType.RIGHT_OUTER;
    case FULL_OUTER_JOIN:
      return JoinType.FULL_OUTER;
    case LEFT_SEMI_JOIN:
      return JoinType.LEFT_SEMI;
    case RIGHT_SEMI_JOIN:
      return JoinType.RIGHT_SEMI;
    case LEFT_ANTI_JOIN:
      return JoinType.LEFT_ANTI;
    case RIGHT_ANTI_JOIN:
      return JoinType.RIGHT_ANTI;
    case UNION_JOIN:
      return JoinType.UNION;
    default:
      throw new RuntimeException("Unknown JoinType: " + type.name());
    }
  }
}
