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
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.exception.NotImplementedException;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.rewrite.rules.IndexScanInfo.SimplePredicate;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.TUtil;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * It deserializes a list of serialized logical nodes into a logical node tree.
 */
public class LogicalNodeDeserializer {

  /**
   * Deserialize a list of nodes into a logical node tree.
   *
   * @param context QueryContext
   * @param tree LogicalNodeTree which contains a list of serialized logical nodes.
   * @return A logical node tree
   */
  public static LogicalNode deserialize(OverridableConf context, @Nullable EvalContext evalContext,
                                        PlanProto.LogicalNodeTree tree) {
    Map<Integer, LogicalNode> nodeMap = Maps.newHashMap();

    // sort serialized logical nodes in an ascending order of their sids
    List<PlanProto.LogicalNode> nodeList = Lists.newArrayList(tree.getNodesList());
    Collections.sort(nodeList, new Comparator<PlanProto.LogicalNode>() {
      @Override
      public int compare(PlanProto.LogicalNode o1, PlanProto.LogicalNode o2) {
        return o1.getVisitSeq() - o2.getVisitSeq();
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
        current = convertEvalExpr(context, evalContext, protoNode);
        break;
      case PROJECTION:
        current = convertProjection(context, evalContext, nodeMap, protoNode);
        break;
      case LIMIT:
        current = convertLimit(nodeMap, protoNode);
        break;
      case SORT:
        current = convertSort(nodeMap, protoNode);
        break;
      case WINDOW_AGG:
        current = convertWindowAgg(context, evalContext, nodeMap, protoNode);
        break;
      case HAVING:
        current = convertHaving(context, evalContext, nodeMap, protoNode);
        break;
      case GROUP_BY:
        current = convertGroupby(context, evalContext, nodeMap, protoNode);
        break;
      case DISTINCT_GROUP_BY:
        current = convertDistinctGroupby(context, evalContext, nodeMap, protoNode);
        break;
      case SELECTION:
        current = convertFilter(context, evalContext, nodeMap, protoNode);
        break;
      case JOIN:
        current = convertJoin(context, evalContext, nodeMap, protoNode);
        break;
      case TABLE_SUBQUERY:
        current = convertTableSubQuery(context, evalContext, nodeMap, protoNode);
        break;
      case UNION:
        current = convertUnion(nodeMap, protoNode);
        break;
      case PARTITIONS_SCAN:
        current = convertPartitionScan(context, evalContext, protoNode);
        break;
      case SCAN:
        current = convertScan(context, evalContext, protoNode);
        break;
      case INDEX_SCAN:
        current = convertIndexScan(context, evalContext, protoNode);
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

      case CREATE_INDEX:
        current = convertCreateIndex(nodeMap, protoNode);
        break;
      case DROP_INDEX:
        current = convertDropIndex(protoNode);
        break;

      default:
        throw new RuntimeException("Unknown NodeType: " + protoNode.getType().name());
      }

      nodeMap.put(protoNode.getVisitSeq(), current);
    }

    return current;
  }

  private static LogicalRootNode convertRoot(Map<Integer, LogicalNode> nodeMap,
                                            PlanProto.LogicalNode protoNode) {
    PlanProto.RootNode rootProto = protoNode.getRoot();

    LogicalRootNode root = new LogicalRootNode(protoNode.getNodeId());
    root.setChild(nodeMap.get(rootProto.getChildSeq()));
    if (protoNode.hasInSchema()) {
      root.setInSchema(convertSchema(protoNode.getInSchema()));
    }
    if (protoNode.hasOutSchema()) {
      root.setOutSchema(convertSchema(protoNode.getOutSchema()));
    }

    return root;
  }

  private static SetSessionNode convertSetSession(PlanProto.LogicalNode protoNode) {
    PlanProto.SetSessionNode setSessionProto = protoNode.getSetSession();

    SetSessionNode setSession = new SetSessionNode(protoNode.getNodeId());
    setSession.init(setSessionProto.getName(), setSessionProto.hasValue() ? setSessionProto.getValue() : null);

    return setSession;
  }

  private static EvalExprNode convertEvalExpr(OverridableConf context, EvalContext evalContext,
                                              PlanProto.LogicalNode protoNode) {
    PlanProto.EvalExprNode evalExprProto = protoNode.getExprEval();

    EvalExprNode evalExpr = new EvalExprNode(protoNode.getNodeId());
    evalExpr.setInSchema(convertSchema(protoNode.getInSchema()));
    evalExpr.setTargets(convertTargets(context, evalContext, evalExprProto.getTargetsList()));

    return evalExpr;
  }

  private static ProjectionNode convertProjection(OverridableConf context, EvalContext evalContext,
                                                  Map<Integer, LogicalNode> nodeMap,
                                                  PlanProto.LogicalNode protoNode) {
    PlanProto.ProjectionNode projectionProto = protoNode.getProjection();

    ProjectionNode projectionNode = new ProjectionNode(protoNode.getNodeId());
    projectionNode.init(projectionProto.getDistinct(), convertTargets(context, evalContext,
        projectionProto.getTargetsList()));
    projectionNode.setChild(nodeMap.get(projectionProto.getChildSeq()));
    projectionNode.setInSchema(convertSchema(protoNode.getInSchema()));
    projectionNode.setOutSchema(convertSchema(protoNode.getOutSchema()));

    return projectionNode;
  }

  private static LimitNode convertLimit(Map<Integer, LogicalNode> nodeMap, PlanProto.LogicalNode protoNode) {
    PlanProto.LimitNode limitProto = protoNode.getLimit();

    LimitNode limitNode = new LimitNode(protoNode.getNodeId());
    limitNode.setChild(nodeMap.get(limitProto.getChildSeq()));
    limitNode.setInSchema(convertSchema(protoNode.getInSchema()));
    limitNode.setOutSchema(convertSchema(protoNode.getOutSchema()));
    limitNode.setFetchFirst(limitProto.getFetchFirstNum());

    return limitNode;
  }

  private static SortNode convertSort(Map<Integer, LogicalNode> nodeMap, PlanProto.LogicalNode protoNode) {
    PlanProto.SortNode sortProto = protoNode.getSort();

    SortNode sortNode = new SortNode(protoNode.getNodeId());
    sortNode.setChild(nodeMap.get(sortProto.getChildSeq()));
    sortNode.setInSchema(convertSchema(protoNode.getInSchema()));
    sortNode.setOutSchema(convertSchema(protoNode.getOutSchema()));
    sortNode.setSortSpecs(convertSortSpecs(sortProto.getSortSpecsList()));

    return sortNode;
  }

  private static HavingNode convertHaving(OverridableConf context, EvalContext evalContext,
                                          Map<Integer, LogicalNode> nodeMap, PlanProto.LogicalNode protoNode) {
    PlanProto.FilterNode havingProto = protoNode.getFilter();

    HavingNode having = new HavingNode(protoNode.getNodeId());
    having.setChild(nodeMap.get(havingProto.getChildSeq()));
    having.setQual(EvalNodeDeserializer.deserialize(context, evalContext, havingProto.getQual()));
    having.setInSchema(convertSchema(protoNode.getInSchema()));
    having.setOutSchema(convertSchema(protoNode.getOutSchema()));

    return having;
  }

  private static WindowAggNode convertWindowAgg(OverridableConf context, EvalContext evalContext,
                                                Map<Integer, LogicalNode> nodeMap,
                                                PlanProto.LogicalNode protoNode) {
    PlanProto.WindowAggNode windowAggProto = protoNode.getWindowAgg();

    WindowAggNode windowAgg = new WindowAggNode(protoNode.getNodeId());
    windowAgg.setChild(nodeMap.get(windowAggProto.getChildSeq()));

    if (windowAggProto.getPartitionKeysCount() > 0) {
      windowAgg.setPartitionKeys(convertColumns(windowAggProto.getPartitionKeysList()));
    }

    if (windowAggProto.getWindowFunctionsCount() > 0) {
      windowAgg.setWindowFunctions(convertWindowFunccEvals(context, evalContext,
          windowAggProto.getWindowFunctionsList()));
    }

    windowAgg.setDistinct(windowAggProto.getDistinct());

    if (windowAggProto.getSortSpecsCount() > 0) {
      windowAgg.setSortSpecs(convertSortSpecs(windowAggProto.getSortSpecsList()));
    }

    if (windowAggProto.getTargetsCount() > 0) {
      windowAgg.setTargets(convertTargets(context, evalContext, windowAggProto.getTargetsList()));
    }

    windowAgg.setInSchema(convertSchema(protoNode.getInSchema()));
    windowAgg.setOutSchema(convertSchema(protoNode.getOutSchema()));

    return windowAgg;
  }

  private static GroupbyNode convertGroupby(OverridableConf context, EvalContext evalContext,
                                            Map<Integer, LogicalNode> nodeMap, PlanProto.LogicalNode protoNode) {
    PlanProto.GroupbyNode groupbyProto = protoNode.getGroupby();

    GroupbyNode groupby = new GroupbyNode(protoNode.getNodeId());
    groupby.setChild(nodeMap.get(groupbyProto.getChildSeq()));
    groupby.setDistinct(groupbyProto.getDistinct());

    if (groupbyProto.getGroupingKeysCount() > 0) {
      groupby.setGroupingColumns(convertColumns(groupbyProto.getGroupingKeysList()));
    }
    if (groupbyProto.getAggFunctionsCount() > 0) {
      groupby.setAggFunctions(convertAggFuncCallEvals(context, evalContext, groupbyProto.getAggFunctionsList()));
    }
    if (groupbyProto.getTargetsCount() > 0) {
      groupby.setTargets(convertTargets(context, evalContext, groupbyProto.getTargetsList()));
    }

    groupby.setInSchema(convertSchema(protoNode.getInSchema()));
    groupby.setOutSchema(convertSchema(protoNode.getOutSchema()));

    return groupby;
  }

  private static DistinctGroupbyNode convertDistinctGroupby(OverridableConf context, EvalContext evalContext,
                                                            Map<Integer, LogicalNode> nodeMap,
                                                            PlanProto.LogicalNode protoNode) {
    PlanProto.DistinctGroupbyNode distinctGroupbyProto = protoNode.getDistinctGroupby();

    DistinctGroupbyNode distinctGroupby = new DistinctGroupbyNode(protoNode.getNodeId());
    distinctGroupby.setChild(nodeMap.get(distinctGroupbyProto.getChildSeq()));

    if (distinctGroupbyProto.hasGroupbyNode()) {
      distinctGroupby.setGroupbyPlan(convertGroupby(context, evalContext, nodeMap,
          distinctGroupbyProto.getGroupbyNode()));
    }

    if (distinctGroupbyProto.getSubPlansCount() > 0) {
      List<GroupbyNode> subPlans = TUtil.newList();
      for (int i = 0; i < distinctGroupbyProto.getSubPlansCount(); i++) {
        subPlans.add(convertGroupby(context, evalContext, nodeMap, distinctGroupbyProto.getSubPlans(i)));
      }
      distinctGroupby.setSubPlans(subPlans);
    }

    if (distinctGroupbyProto.getGroupingKeysCount() > 0) {
      distinctGroupby.setGroupingColumns(convertColumns(distinctGroupbyProto.getGroupingKeysList()));
    }
    if (distinctGroupbyProto.getAggFunctionsCount() > 0) {
      distinctGroupby.setAggFunctions(convertAggFuncCallEvals(context, evalContext,
          distinctGroupbyProto.getAggFunctionsList()));
    }
    if (distinctGroupbyProto.getTargetsCount() > 0) {
      distinctGroupby.setTargets(convertTargets(context, evalContext, distinctGroupbyProto.getTargetsList()));
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

  private static JoinNode convertJoin(OverridableConf context, EvalContext evalContext,
                                      Map<Integer, LogicalNode> nodeMap, PlanProto.LogicalNode protoNode) {
    PlanProto.JoinNode joinProto = protoNode.getJoin();

    JoinNode join = new JoinNode(protoNode.getNodeId());
    join.setLeftChild(nodeMap.get(joinProto.getLeftChildSeq()));
    join.setRightChild(nodeMap.get(joinProto.getRightChilSeq()));
    join.setJoinType(convertJoinType(joinProto.getJoinType()));
    join.setInSchema(convertSchema(protoNode.getInSchema()));
    join.setOutSchema(convertSchema(protoNode.getOutSchema()));
    if (joinProto.hasJoinQual()) {
      join.setJoinQual(EvalNodeDeserializer.deserialize(context, evalContext, joinProto.getJoinQual()));
    }
    if (joinProto.getExistsTargets()) {
      join.setTargets(convertTargets(context, evalContext, joinProto.getTargetsList()));
    }

    return join;
  }

  private static SelectionNode convertFilter(OverridableConf context, EvalContext evalContext,
                                             Map<Integer, LogicalNode> nodeMap, PlanProto.LogicalNode protoNode) {
    PlanProto.FilterNode filterProto = protoNode.getFilter();

    SelectionNode selection = new SelectionNode(protoNode.getNodeId());
    selection.setInSchema(convertSchema(protoNode.getInSchema()));
    selection.setOutSchema(convertSchema(protoNode.getOutSchema()));
    selection.setChild(nodeMap.get(filterProto.getChildSeq()));
    selection.setQual(EvalNodeDeserializer.deserialize(context, evalContext, filterProto.getQual()));

    return selection;
  }

  private static UnionNode convertUnion(Map<Integer, LogicalNode> nodeMap, PlanProto.LogicalNode protoNode) {
    PlanProto.UnionNode unionProto = protoNode.getUnion();

    UnionNode union = new UnionNode(protoNode.getNodeId());
    union.setInSchema(convertSchema(protoNode.getInSchema()));
    union.setOutSchema(convertSchema(protoNode.getOutSchema()));
    union.setLeftChild(nodeMap.get(unionProto.getLeftChildSeq()));
    union.setRightChild(nodeMap.get(unionProto.getRightChildSeq()));

    return union;
  }

  private static ScanNode convertScan(OverridableConf context, EvalContext evalContext, PlanProto.LogicalNode protoNode) {
    ScanNode scan = new ScanNode(protoNode.getNodeId());
    fillScanNode(context, evalContext, protoNode, scan);

    return scan;
  }

  private static void fillScanNode(OverridableConf context, EvalContext evalContext, PlanProto.LogicalNode protoNode,
                                   ScanNode scan) {
    PlanProto.ScanNode scanProto = protoNode.getScan();
    if (scanProto.hasAlias()) {
      scan.init(new TableDesc(scanProto.getTable()), scanProto.getAlias());
    } else {
      scan.init(new TableDesc(scanProto.getTable()));
    }

    if (scanProto.getExistTargets()) {
      scan.setTargets(convertTargets(context, evalContext, scanProto.getTargetsList()));
    }

    if (scanProto.hasQual()) {
      scan.setQual(EvalNodeDeserializer.deserialize(context, evalContext, scanProto.getQual()));
    }

    if(scanProto.hasBroadcast()){
      scan.setBroadcastTable(scanProto.getBroadcast());
    }
    scan.setInSchema(convertSchema(protoNode.getInSchema()));
    scan.setOutSchema(convertSchema(protoNode.getOutSchema()));
    scan.setNameResolveBase(scanProto.getNameResolveBase());
  }

  private static IndexScanNode convertIndexScan(OverridableConf context, EvalContext evalContext,
                                                PlanProto.LogicalNode protoNode) {
    IndexScanNode indexScan = new IndexScanNode(protoNode.getNodeId());
    fillScanNode(context, evalContext, protoNode, indexScan);

    PlanProto.IndexScanSpec indexScanSpec = protoNode.getIndexScan();
    SimplePredicate[] predicates = new SimplePredicate[indexScanSpec.getPredicatesCount()];
    for (int i = 0; i < predicates.length; i++) {
      predicates[i] = new SimplePredicate(indexScanSpec.getPredicates(i));
    }

    indexScan.set(new Schema(indexScanSpec.getKeySchema()), predicates,
        TUtil.stringToURI(indexScanSpec.getIndexPath()));

    return indexScan;
  }

  private static PartitionedTableScanNode convertPartitionScan(OverridableConf context, EvalContext evalContext,
                                                               PlanProto.LogicalNode protoNode) {
    PartitionedTableScanNode partitionedScan = new PartitionedTableScanNode(protoNode.getNodeId());
    fillScanNode(context, evalContext, protoNode, partitionedScan);

    PlanProto.PartitionScanSpec partitionScanProto = protoNode.getPartitionScan();
    Path [] paths = new Path[partitionScanProto.getPathsCount()];
    for (int i = 0; i < partitionScanProto.getPathsCount(); i++) {
      paths[i] = new Path(partitionScanProto.getPaths(i));
    }
    partitionedScan.setInputPaths(paths);
    return partitionedScan;
  }

  private static TableSubQueryNode convertTableSubQuery(OverridableConf context, EvalContext evalContext,
                                                        Map<Integer, LogicalNode> nodeMap,
                                                        PlanProto.LogicalNode protoNode) {
    PlanProto.TableSubQueryNode proto = protoNode.getTableSubQuery();

    TableSubQueryNode tableSubQuery = new TableSubQueryNode(protoNode.getNodeId());
    tableSubQuery.init(proto.getTableName(), nodeMap.get(proto.getChildSeq()));
    tableSubQuery.setInSchema(convertSchema(protoNode.getInSchema()));
    if (proto.getTargetsCount() > 0) {
      tableSubQuery.setTargets(convertTargets(context, evalContext, proto.getTargetsList()));
    }
    tableSubQuery.setNameResolveBase(proto.getNameResolveBase());

    return tableSubQuery;
  }

  private static CreateTableNode convertCreateTable(Map<Integer, LogicalNode> nodeMap,
                                            PlanProto.LogicalNode protoNode) {
    PlanProto.PersistentStoreNode persistentStoreProto = protoNode.getPersistentStore();
    PlanProto.StoreTableNodeSpec storeTableNodeSpec = protoNode.getStoreTable();
    PlanProto.CreateTableNodeSpec createTableNodeSpec = protoNode.getCreateTable();

    CreateTableNode createTable = new CreateTableNode(protoNode.getNodeId());
    if (protoNode.hasInSchema()) {
      createTable.setInSchema(convertSchema(protoNode.getInSchema()));
    }
    if (protoNode.hasOutSchema()) {
      createTable.setOutSchema(convertSchema(protoNode.getOutSchema()));
    }
    createTable.setChild(nodeMap.get(persistentStoreProto.getChildSeq()));
    createTable.setDataFormat(persistentStoreProto.getStorageType());
    createTable.setOptions(new KeyValueSet(persistentStoreProto.getTableProperties()));

    createTable.setTableName(storeTableNodeSpec.getTableName());
    if (storeTableNodeSpec.hasPartitionMethod()) {
      createTable.setPartitionMethod(new PartitionMethodDesc(storeTableNodeSpec.getPartitionMethod()));
    }

    if (storeTableNodeSpec.hasTableSchema()) {
      createTable.setTableSchema(convertSchema(storeTableNodeSpec.getTableSchema()));
    }

    if (createTableNodeSpec.hasTablespaceName()) {
     createTable.setTableSpaceName(createTableNodeSpec.getTablespaceName());
    }
    createTable.setExternal(createTableNodeSpec.getExternal());
    if (storeTableNodeSpec.hasUri()) {
      createTable.setUri(URI.create(storeTableNodeSpec.getUri()));
    }
    createTable.setIfNotExists(createTableNodeSpec.getIfNotExists());

    return createTable;
  }

  private static InsertNode convertInsert(Map<Integer, LogicalNode> nodeMap,
                                                   PlanProto.LogicalNode protoNode) {
    PlanProto.PersistentStoreNode persistentStoreProto = protoNode.getPersistentStore();
    PlanProto.StoreTableNodeSpec storeTableNodeSpec = protoNode.getStoreTable();
    PlanProto.InsertNodeSpec insertNodeSpec = protoNode.getInsert();

    InsertNode insertNode = new InsertNode(protoNode.getNodeId());
    if (protoNode.hasInSchema()) {
      insertNode.setInSchema(convertSchema(protoNode.getInSchema()));
    }
    if (protoNode.hasOutSchema()) {
      insertNode.setOutSchema(convertSchema(protoNode.getOutSchema()));
    }
    insertNode.setChild(nodeMap.get(persistentStoreProto.getChildSeq()));
    insertNode.setDataFormat(persistentStoreProto.getStorageType());
    insertNode.setOptions(new KeyValueSet(persistentStoreProto.getTableProperties()));

    if (storeTableNodeSpec.hasTableName()) {
      insertNode.setTableName(storeTableNodeSpec.getTableName());
    }
    if (storeTableNodeSpec.hasPartitionMethod()) {
      insertNode.setPartitionMethod(new PartitionMethodDesc(storeTableNodeSpec.getPartitionMethod()));
    }

    insertNode.setOverwrite(insertNodeSpec.getOverwrite());
    insertNode.setTableSchema(convertSchema(storeTableNodeSpec.getTableSchema()));
    if (insertNodeSpec.hasTargetSchema()) {
      insertNode.setTargetSchema(convertSchema(insertNodeSpec.getTargetSchema()));
    }
    if (insertNodeSpec.hasProjectedSchema()) {
      insertNode.setProjectedSchema(convertSchema(insertNodeSpec.getProjectedSchema()));
    }
    insertNode.setUri(URI.create(storeTableNodeSpec.getUri()));

    return insertNode;
  }

  private static DropTableNode convertDropTable(PlanProto.LogicalNode protoNode) {
    DropTableNode dropTable = new DropTableNode(protoNode.getNodeId());

    PlanProto.DropTableNode dropTableProto = protoNode.getDropTable();
    dropTable.init(dropTableProto.getTableName(), dropTableProto.getIfExists(), dropTableProto.getPurge());

    return dropTable;
  }

  private static CreateDatabaseNode convertCreateDatabase(PlanProto.LogicalNode protoNode) {
    CreateDatabaseNode createDatabase = new CreateDatabaseNode(protoNode.getNodeId());

    PlanProto.CreateDatabaseNode createDatabaseProto = protoNode.getCreateDatabase();
    createDatabase.init(createDatabaseProto.getDbName(), createDatabaseProto.getIfNotExists());

    return createDatabase;
  }

  private static DropDatabaseNode convertDropDatabase(PlanProto.LogicalNode protoNode) {
    DropDatabaseNode dropDatabase = new DropDatabaseNode(protoNode.getNodeId());

    PlanProto.DropDatabaseNode dropDatabaseProto = protoNode.getDropDatabase();
    dropDatabase.init(dropDatabaseProto.getDbName(), dropDatabaseProto.getIfExists());

    return dropDatabase;
  }

  private static AlterTablespaceNode convertAlterTablespace(PlanProto.LogicalNode protoNode) {
    AlterTablespaceNode alterTablespace = new AlterTablespaceNode(protoNode.getNodeId());

    PlanProto.AlterTablespaceNode alterTablespaceProto = protoNode.getAlterTablespace();
    alterTablespace.setTablespaceName(alterTablespaceProto.getTableSpaceName());

    switch (alterTablespaceProto.getSetType()) {
    case LOCATION:
      alterTablespace.setLocation(alterTablespaceProto.getSetLocation().getLocation());
      break;
    default:
      throw new TajoRuntimeException(
          new NotImplementedException("Unknown SET type in ALTER TABLE: " + alterTablespaceProto.getSetType().name()));
    }

    return alterTablespace;
  }

  private static AlterTableNode convertAlterTable(PlanProto.LogicalNode protoNode) {
    AlterTableNode alterTable = new AlterTableNode(protoNode.getNodeId());

    PlanProto.AlterTableNode alterTableProto = protoNode.getAlterTable();
    alterTable.setTableName(alterTableProto.getTableName());

    PlanProto.AlterTableNode.AlterPartition alterPartition = null;

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
    case SET_PROPERTY:
      alterTable.setProperties(new KeyValueSet(alterTableProto.getProperties()));
      break;
    case ADD_PARTITION:
      alterPartition = alterTableProto.getAlterPartition();
      alterTable.setPartitionColumns(alterPartition.getColumnNamesList().toArray(new String[alterPartition
      .getColumnNamesCount()]));
      alterTable.setPartitionValues(alterPartition.getPartitionValuesList().toArray(new String[alterPartition
        .getPartitionValuesCount()]));
      if (alterPartition.getLocation() != null) {
        alterTable.setLocation(alterPartition.getLocation());
      }
      alterTable.setIfNotExists(alterPartition.getIfNotExists());
      break;
    case DROP_PARTITION:
      alterPartition = alterTableProto.getAlterPartition();
      alterTable.setPartitionColumns(alterPartition.getColumnNamesList().toArray(new String[alterPartition
        .getColumnNamesCount()]));
      alterTable.setPartitionValues(alterPartition.getPartitionValuesList().toArray(new String[alterPartition
        .getPartitionValuesCount()]));
      alterTable.setPurge(alterPartition.getPurge());
      alterTable.setIfExists(alterPartition.getIfExists());
      break;
    case REPAIR_PARTITION:
      alterTable.setTableName(alterTableProto.getTableName());
      break;
    default:
      throw new TajoRuntimeException(
          new NotImplementedException("Unknown SET type in ALTER TABLE: " + alterTableProto.getSetType().name()));
    }

    return alterTable;
  }

  private static TruncateTableNode convertTruncateTable(PlanProto.LogicalNode protoNode) {
    TruncateTableNode truncateTable = new TruncateTableNode(protoNode.getNodeId());

    PlanProto.TruncateTableNode truncateTableProto = protoNode.getTruncateTableNode();
    truncateTable.setTableNames(truncateTableProto.getTableNamesList());

    return truncateTable;
  }

  private static CreateIndexNode convertCreateIndex(Map<Integer, LogicalNode> nodeMap,
                                                    PlanProto.LogicalNode protoNode) {
    CreateIndexNode createIndex = new CreateIndexNode(protoNode.getNodeId());

    PlanProto.CreateIndexNode createIndexProto = protoNode.getCreateIndex();
    createIndex.setIndexName(createIndexProto.getIndexName());
    createIndex.setIndexMethod(createIndexProto.getIndexMethod());
    try {
      createIndex.setIndexPath(new URI(createIndexProto.getIndexPath()));
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    SortSpec[] keySortSpecs = new SortSpec[createIndexProto.getKeySortSpecsCount()];
    for (int i = 0; i < keySortSpecs.length; i++) {
      keySortSpecs[i] = new SortSpec(createIndexProto.getKeySortSpecs(i));
    }
    createIndex.setKeySortSpecs(new Schema(createIndexProto.getTargetRelationSchema()),
        keySortSpecs);
    createIndex.setUnique(createIndexProto.getIsUnique());
    createIndex.setClustered(createIndexProto.getIsClustered());
    if (createIndexProto.hasIndexProperties()) {
      createIndex.setOptions(new KeyValueSet(createIndexProto.getIndexProperties()));
    }
    createIndex.setChild(nodeMap.get(createIndexProto.getChildSeq()));
    createIndex.setInSchema(convertSchema(protoNode.getInSchema()));
    createIndex.setOutSchema(convertSchema(protoNode.getOutSchema()));
    createIndex.setExternal(createIndexProto.getIsExternal());

    return createIndex;
  }

  private static DropIndexNode convertDropIndex(PlanProto.LogicalNode protoNode) {
    DropIndexNode dropIndex = new DropIndexNode(protoNode.getNodeId());

    PlanProto.DropIndexNode dropIndexProto = protoNode.getDropIndex();
    dropIndex.setIndexName(dropIndexProto.getIndexName());

    return dropIndex;
  }

  private static List<AggregationFunctionCallEval> convertAggFuncCallEvals(OverridableConf context, EvalContext evalContext,
                                                                       List<PlanProto.EvalNodeTree> evalTrees) {
    List<AggregationFunctionCallEval> aggFuncs = new ArrayList<>();
    for (int i = 0; i < evalTrees.size(); i++) {
      aggFuncs.add((AggregationFunctionCallEval) EvalNodeDeserializer.deserialize(context, evalContext,
          evalTrees.get(i)));
    }
    return aggFuncs;
  }

  private static WindowFunctionEval[] convertWindowFunccEvals(OverridableConf context, EvalContext evalContext,
                                                              List<PlanProto.EvalNodeTree> evalTrees) {
    WindowFunctionEval[] winFuncEvals = new WindowFunctionEval[evalTrees.size()];
    for (int i = 0; i < winFuncEvals.length; i++) {
      winFuncEvals[i] = (WindowFunctionEval) EvalNodeDeserializer.deserialize(context, evalContext, evalTrees.get(i));
    }
    return winFuncEvals;
  }

  public static Schema convertSchema(CatalogProtos.SchemaProto proto) {
    return new Schema(proto);
  }

  public static Column[] convertColumns(List<CatalogProtos.ColumnProto> columnProtos) {
    Column [] columns = new Column[columnProtos.size()];
    for (int i = 0; i < columns.length; i++) {
      columns[i] = new Column(columnProtos.get(i));
    }
    return columns;
  }

  public static List<Target> convertTargets(OverridableConf context, EvalContext evalContext,
                                        List<PlanProto.Target> targetsProto) {
    List<Target> targets = new ArrayList<>();
    for (PlanProto.Target targetProto : targetsProto) {
      EvalNode evalNode = EvalNodeDeserializer.deserialize(context, evalContext, targetProto.getExpr());

      if (targetProto.hasAlias()) {
        targets.add(new Target(evalNode, targetProto.getAlias()));
      } else {
        targets.add(new Target((FieldEval) evalNode));
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
