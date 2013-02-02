/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.master;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.event.EventHandler;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import tajo.QueryId;
import tajo.QueryIdFactory;
import tajo.QueryUnitAttemptId;
import tajo.SubQueryId;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.catalog.statistics.TableStat;
import tajo.common.exception.NotImplementedException;
import tajo.conf.TajoConf;
import tajo.engine.MasterWorkerProtos.Partition;
import tajo.engine.parser.QueryBlock.FromTable;
import tajo.engine.planner.PlannerUtil;
import tajo.engine.planner.RangePartitionAlgorithm;
import tajo.engine.planner.UniformRangePartition;
import tajo.engine.planner.global.MasterPlan;
import tajo.engine.planner.logical.*;
import tajo.engine.utils.TupleUtil;
import tajo.master.SubQuery.PARTITION_TYPE;
import tajo.storage.Fragment;
import tajo.storage.StorageManager;
import tajo.storage.TupleRange;
import tajo.util.TajoIdUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.Map.Entry;

public class GlobalPlanner {
  private static Log LOG = LogFactory.getLog(GlobalPlanner.class);

  private TajoConf conf;
  private StorageManager sm;
  private CatalogService catalog;
  private QueryId queryId;
  private EventHandler eventHandler;

  public GlobalPlanner(final TajoConf conf, final CatalogService catalog,
                       final StorageManager sm,
                       final EventHandler eventHandler)
      throws IOException {
    this.conf = conf;
    this.sm = sm;
    this.catalog = catalog;
    this.eventHandler = eventHandler;
  }

  /**
   * Builds a master plan from the given logical plan.
   * @param queryId
   * @param rootNode
   * @return
   * @throws IOException
   */
  public MasterPlan build(QueryId queryId, LogicalRootNode rootNode)
      throws IOException {
    this.queryId = queryId;

    String outputTableName = null;
    if (rootNode.getSubNode().getType() == ExprType.STORE) {
      // create table queries are executed by the master
      StoreTableNode storeTableNode = (StoreTableNode) rootNode.getSubNode();
      outputTableName = storeTableNode.getTableName();
    }

    // insert store at the subnode of the root
    UnaryNode root = rootNode;
    IndexWriteNode indexNode = null;
    // TODO: check whether the type of the subnode is CREATE_INDEX
    if (root.getSubNode().getType() == ExprType.CREATE_INDEX) {
      indexNode = (IndexWriteNode) root.getSubNode();
      root = (UnaryNode)root.getSubNode();
      
      StoreIndexNode store = new StoreIndexNode(
          QueryIdFactory.newSubQueryId(this.queryId).toString());
      store.setLocal(false);
      PlannerUtil.insertNode(root, store);
      
    } else if (root.getSubNode().getType() != ExprType.STORE) {
      SubQueryId subQueryId = QueryIdFactory.newSubQueryId(this.queryId);
      outputTableName = subQueryId.toString();
      insertStore(subQueryId.toString(),root).setLocal(false);
    }
    
    // convert 2-phase plan
    LogicalNode tp = convertTo2Phase(rootNode);

    // make query graph
    MasterPlan globalPlan = convertToGlobalPlan(indexNode, tp);
    globalPlan.setOutputTableName(outputTableName);

    return globalPlan;
  }
  
  private StoreTableNode insertStore(String tableId, LogicalNode parent) {
    StoreTableNode store = new StoreTableNode(tableId);
    store.setLocal(true);
    PlannerUtil.insertNode(parent, store);
    return store;
  }

  public int getTaskNum(SubQuery subQuery) {
    int numTasks;
    GroupbyNode grpNode = (GroupbyNode) PlannerUtil.findTopNode(
        subQuery.getLogicalPlan(), ExprType.GROUP_BY);
    if (subQuery.getParentQuery() == null && grpNode != null
        && grpNode.getGroupingColumns().length == 0) {
      numTasks = 1;
    } else {
      // TODO - to be improved
      numTasks = 32;
    }
    return numTasks;
  }
  
  /**
   * Transforms a logical plan to a two-phase plan. 
   * Store nodes are inserted for every logical nodes except store and scan nodes
   * 
   * @author jihoon
   *
   */
  private class GlobalPlanBuilder implements LogicalNodeVisitor {
    @Override
    public void visit(LogicalNode node) {
      String tableId;
      StoreTableNode store;
      if (node.getType() == ExprType.GROUP_BY) {
        // transform group by to two-phase plan 
        GroupbyNode groupby = (GroupbyNode) node;
        // insert a store for the child of first group by
        if (groupby.getSubNode().getType() != ExprType.UNION &&
            groupby.getSubNode().getType() != ExprType.STORE &&
            groupby.getSubNode().getType() != ExprType.SCAN) {
          tableId = QueryIdFactory.newSubQueryId(queryId).toString();
          insertStore(tableId, groupby);
        }
        tableId = QueryIdFactory.newSubQueryId(queryId).toString();
        // insert (a store for the first group by) and (a second group by)
        PlannerUtil.transformGroupbyTo2PWithStore((GroupbyNode)node, tableId);
      } else if (node.getType() == ExprType.SORT) {
        // transform sort to two-phase plan 
        SortNode sort = (SortNode) node;
        // insert a store for the child of first sort
        if (sort.getSubNode().getType() != ExprType.UNION &&
            sort.getSubNode().getType() != ExprType.STORE &&
            sort.getSubNode().getType() != ExprType.SCAN) {
          tableId = QueryIdFactory.newSubQueryId(queryId).toString();
          insertStore(tableId, sort);
        }
        tableId = QueryIdFactory.newSubQueryId(queryId).toString();
        // insert (a store for the first sort) and (a second sort)
        PlannerUtil.transformSortTo2PWithStore((SortNode)node, tableId);
      } else if (node.getType() == ExprType.JOIN) {
        // transform join to two-phase plan 
        // the first phase of two-phase join can be any logical nodes
        JoinNode join = (JoinNode) node;

        /*
        if (join.getOuterNode().getType() == ExprType.SCAN &&
            join.getInnerNode().getType() == ExprType.SCAN) {
          ScanNode outerScan = (ScanNode) join.getOuterNode();
          ScanNode innerScan = (ScanNode) join.getInnerNode();


          TableMeta outerMeta =
              catalog.getTableDesc(outerScan.getTableId()).getMeta();
          TableMeta innerMeta =
              catalog.getTableDesc(innerScan.getTableId()).getMeta();
          long threshold = conf.getLongVar(ConfVars.BROADCAST_JOIN_THRESHOLD);


          // if the broadcast join is available
          boolean outerSmall = false;
          boolean innerSmall = false;
          if (!outerScan.isLocal() && outerMeta.getStat() != null &&
              outerMeta.getStat().getNumBytes() <= threshold) {
            outerSmall = true;
            LOG.info("The relation (" + outerScan.getTableId() +
                ") is less than " + threshold);
          }
          if (!innerScan.isLocal() && innerMeta.getStat() != null &&
              innerMeta.getStat().getNumBytes() <= threshold) {
            innerSmall = true;
            LOG.info("The relation (" + innerScan.getTableId() +
                ") is less than " + threshold);
          }

          if (outerSmall && innerSmall) {
            if (outerMeta.getStat().getNumBytes() <=
                innerMeta.getStat().getNumBytes()) {
              outerScan.setBroadcast();
              LOG.info("The relation " + outerScan.getTableId()
                  + " is broadcasted");
            } else {
              innerScan.setBroadcast();
              LOG.info("The relation " + innerScan.getTableId()
                  + " is broadcasted");
            }
          } else {
            if (outerSmall) {
              outerScan.setBroadcast();
              LOG.info("The relation (" + outerScan.getTableId()
                  + ") is broadcasted");
            } else if (innerSmall) {
              innerScan.setBroadcast();
              LOG.info("The relation (" + innerScan.getTableId()
                  + ") is broadcasted");
            }
          }

          if (outerScan.isBroadcast() || innerScan.isBroadcast()) {
            return;
          }
        } */

        // insert stores for the first phase
        if (join.getOuterNode().getType() != ExprType.UNION &&
            join.getOuterNode().getType() != ExprType.STORE) {
          tableId = QueryIdFactory.newSubQueryId(queryId).toString();
          store = new StoreTableNode(tableId);
          store.setLocal(true);
          PlannerUtil.insertOuterNode(node, store);
        }
        if (join.getInnerNode().getType() != ExprType.UNION &&
            join.getInnerNode().getType() != ExprType.STORE) {
          tableId = QueryIdFactory.newSubQueryId(queryId).toString();
          store = new StoreTableNode(tableId);
          store.setLocal(true);
          PlannerUtil.insertInnerNode(node, store);
        }
      } else if (node.getType() == ExprType.UNION) {
        // not two-phase transform
        UnionNode union = (UnionNode) node;
        // insert stores
        if (union.getOuterNode().getType() != ExprType.UNION &&
            union.getOuterNode().getType() != ExprType.STORE) {
          tableId = QueryIdFactory.newSubQueryId(queryId).toString();
          store = new StoreTableNode(tableId);
          if(union.getOuterNode().getType() == ExprType.GROUP_BY) {
            /*This case is for cube by operator
             * TODO : more complicated conidtion*/
            store.setLocal(true);
          } else {
            /* This case is for union query*/
            store.setLocal(false);
          }
          PlannerUtil.insertOuterNode(node, store);
        }
        if (union.getInnerNode().getType() != ExprType.UNION &&
            union.getInnerNode().getType() != ExprType.STORE) {
          tableId = QueryIdFactory.newSubQueryId(queryId).toString();
          store = new StoreTableNode(tableId);
          if(union.getInnerNode().getType() == ExprType.GROUP_BY) {
            /*This case is for cube by operator
             * TODO : more complicated conidtion*/
            store.setLocal(true);
          }else {
            /* This case is for union query*/
            store.setLocal(false);
          }
          PlannerUtil.insertInnerNode(node, store);
        }
      } else if (node instanceof UnaryNode) {
        UnaryNode unary = (UnaryNode)node;
        if (unary.getType() != ExprType.STORE &&
            unary.getSubNode().getType() != ExprType.STORE) {
          tableId = QueryIdFactory.newSubQueryId(queryId).toString();
          insertStore(tableId, unary);
        }
      }
    }
  }

  /**
   * Convert the logical plan to a two-phase plan by the post-order traverse.
   * 
   * @param logicalPlan
   * @return
   */
  private LogicalNode convertTo2Phase(LogicalNode logicalPlan) {
    LogicalRootNode root = (LogicalRootNode) logicalPlan;
    root.postOrder(new GlobalPlanBuilder());
    return logicalPlan;
  }
  
  private Map<StoreTableNode, SubQuery> convertMap =
      new HashMap<StoreTableNode, SubQuery>();
  
  /**
   * Logical plan을 후위 탐색하면서 SubQuery 생성
   * 
   * @param node 현재 방문 중인 노드
   * @throws IOException
   */
  private void recursiveBuildSubQuery(LogicalNode node)
      throws IOException {
    SubQuery subQuery;
    StoreTableNode store;
    if (node instanceof UnaryNode) {
      recursiveBuildSubQuery(((UnaryNode) node).getSubNode());
      
      if (node.getType() == ExprType.STORE) {
        store = (StoreTableNode) node;
        SubQueryId id;
        if (store.getTableName().startsWith(QueryId.PREFIX)) {
          id = TajoIdUtils.newSubQueryId(store.getTableName());
        } else {
          id = QueryIdFactory.newSubQueryId(queryId);
        }
        subQuery = new SubQuery(id, sm, this);

        switch (store.getSubNode().getType()) {
        case BST_INDEX_SCAN:
        case SCAN:  // store - scan
          subQuery = makeScanSubQuery(subQuery);
          subQuery.setLogicalPlan(node);
          break;
        case SELECTION:
        case PROJECTION:
        case LIMIT:
          subQuery = makeUnarySubQuery(store, node, subQuery);
          subQuery.setLogicalPlan(node);
          break;
        case GROUP_BY:
          subQuery = makeGroupbySubQuery(store, node, subQuery);
          subQuery.setLogicalPlan(node);
          break;
        case SORT:
          subQuery = makeSortSubQuery(store, node, subQuery);
          subQuery.setLogicalPlan(node);
          break;
        case JOIN:  // store - join
          subQuery = makeJoinSubQuery(store, node, subQuery);
          subQuery.setLogicalPlan(node);
          break;
        case UNION:
          subQuery = makeUnionSubQuery(store, node, subQuery);
          subQuery.setLogicalPlan(node);
          break;
        default:
          subQuery = null;
          break;
        }

        if (!subQuery.hasChildQuery()) {
          subQuery.setLeafQuery();
        }
        convertMap.put(store, subQuery);
      }
    } else if (node instanceof BinaryNode) {
      recursiveBuildSubQuery(((BinaryNode) node).getOuterNode());
      recursiveBuildSubQuery(((BinaryNode) node).getInnerNode());
    } else if (node instanceof ScanNode) {

    } else {

    }
  }
  
  private SubQuery makeScanSubQuery(SubQuery unit) {
    unit.setOutputType(PARTITION_TYPE.LIST);
    return unit;
  }
  
  private SubQuery makeBSTIndexUnit(LogicalNode plan, SubQuery unit) {
    switch(((IndexWriteNode)plan).getSubNode().getType()){
    case SCAN:
      unit = makeScanSubQuery(unit);
      unit.setLogicalPlan(((IndexWriteNode)plan).getSubNode());
    }
    return unit;
  }
  
  /**
   * Unifiable node(selection, projection)을 자식 플랜과 같은 SubQuery로 생성
   * 
   * @param rootStore 생성할 SubQuery의 store
   * @param plan logical plan
   * @param unit 생성할 SubQuery
   * @return
   * @throws IOException
   */
  private SubQuery makeUnarySubQuery(StoreTableNode rootStore,
                                     LogicalNode plan, SubQuery unit) throws IOException {
    ScanNode newScan;
    SubQuery prev;
    UnaryNode unary = (UnaryNode) plan;
    UnaryNode child = (UnaryNode) unary.getSubNode();
    StoreTableNode prevStore = (StoreTableNode)child.getSubNode();

    // add scan
    newScan = GlobalPlannerUtils.newScanPlan(prevStore.getOutSchema(),
        prevStore.getTableName(), sm.getTablePath(prevStore.getTableName()));
    newScan.setLocal(true);
    child.setSubNode(newScan);
    prev = convertMap.get(prevStore);

    if (prev != null) {
      prev.setParentQuery(unit);
      unit.addChildQuery(newScan, prev);
      prev.setOutputType(PARTITION_TYPE.LIST);
    }

    unit.setOutputType(PARTITION_TYPE.LIST);

    return unit;
  }
  
  /**
   * Two-phase SubQuery 생성.
   * 
   * @param rootStore 생성할 SubQuery의 store
   * @param plan logical plan
   * @param unit 생성할 SubQuery
   * @return
   * @throws IOException
   */
  private SubQuery makeGroupbySubQuery(StoreTableNode rootStore,
                                       LogicalNode plan, SubQuery unit) throws IOException {
    UnaryNode unary = (UnaryNode) plan;
    UnaryNode unaryChild;
    StoreTableNode prevStore;
    ScanNode newScan;
    SubQuery prev;
    unaryChild = (UnaryNode) unary.getSubNode();  // groupby
    ExprType curType = unaryChild.getType();
    if (unaryChild.getSubNode().getType() == ExprType.STORE) {
      // store - groupby - store
      unaryChild = (UnaryNode) unaryChild.getSubNode(); // store
      prevStore = (StoreTableNode) unaryChild;
      newScan = GlobalPlannerUtils.newScanPlan(prevStore.getOutSchema(),
          prevStore.getTableName(),
          sm.getTablePath(prevStore.getTableName()));
      newScan.setLocal(true);
      ((UnaryNode) unary.getSubNode()).setSubNode(newScan);
      prev = convertMap.get(prevStore);
      if (prev != null) {
        prev.setParentQuery(unit);
        unit.addChildQuery(newScan, prev);
      }

      if (unaryChild.getSubNode().getType() == curType) {
        // the second phase
        unit.setOutputType(PARTITION_TYPE.LIST);
        if (prev != null) {
          prev.setOutputType(PARTITION_TYPE.HASH);
        }
      } else {
        // the first phase
        unit.setOutputType(PARTITION_TYPE.HASH);
        if (prev != null) {
          prev.setOutputType(PARTITION_TYPE.LIST);
        }
      }
    } else if (unaryChild.getSubNode().getType() == ExprType.SCAN) {
      // the first phase
      // store - groupby - scan
      unit.setOutputType(PARTITION_TYPE.HASH);
    } else if (unaryChild.getSubNode().getType() == ExprType.UNION) {
      _handleUnionNode(rootStore, (UnionNode)unaryChild.getSubNode(), unit, 
          null, PARTITION_TYPE.LIST);
    } else {
      // error
    }
    return unit;
  }
  
  /**
   * 
   * 
   * @param rootStore 생성할 SubQuery의 store
   * @param plan logical plan
   * @param unit 생성할 SubQuery
   * @return
   * @throws IOException
   */
  private SubQuery makeUnionSubQuery(StoreTableNode rootStore,
                                     LogicalNode plan, SubQuery unit) throws IOException {
    UnaryNode unary = (UnaryNode) plan;
    StoreTableNode outerStore, innerStore;
    SubQuery prev;
    UnionNode union = (UnionNode) unary.getSubNode();
    unit.setOutputType(PARTITION_TYPE.LIST);
    
    if (union.getOuterNode().getType() == ExprType.STORE) {
      outerStore = (StoreTableNode) union.getOuterNode();
      TableMeta outerMeta = TCatUtil.newTableMeta(outerStore.getOutSchema(),
          StoreType.CSV);
      insertOuterScan(union, outerStore.getTableName(), outerMeta);
      prev = convertMap.get(outerStore);
      if (prev != null) {
        prev.getStoreTableNode().setTableName(rootStore.getTableName());
        prev.setOutputType(PARTITION_TYPE.LIST);
        prev.setParentQuery(unit);
        prev.setLeafQuery();
        unit.addChildQuery((ScanNode)union.getOuterNode(), prev);
      }
    } else if (union.getOuterNode().getType() == ExprType.UNION) {
      _handleUnionNode(rootStore, union, unit, null, PARTITION_TYPE.LIST);
    }
    
    if (union.getInnerNode().getType() == ExprType.STORE) {
      innerStore = (StoreTableNode) union.getInnerNode();
      TableMeta innerMeta = TCatUtil.newTableMeta(innerStore.getOutSchema(),
          StoreType.CSV);
      insertInnerScan(union, innerStore.getTableName(), innerMeta);
      prev = convertMap.get(innerStore);
      if (prev != null) {
        prev.getStoreTableNode().setTableName(rootStore.getTableName());
        prev.setOutputType(PARTITION_TYPE.LIST);
        prev.setParentQuery(unit);
        prev.setLeafQuery();
        unit.addChildQuery((ScanNode)union.getInnerNode(), prev);
      }
    } else if (union.getInnerNode().getType() == ExprType.UNION) {
      _handleUnionNode(rootStore, union, unit, null, PARTITION_TYPE.LIST);
    }

    return unit;
  }

  private SubQuery makeSortSubQuery(StoreTableNode rootStore,
                                    LogicalNode plan, SubQuery unit) throws IOException {

    UnaryNode unary = (UnaryNode) plan;
    UnaryNode unaryChild;
    StoreTableNode prevStore;
    ScanNode newScan;
    SubQuery prev;
    unaryChild = (UnaryNode) unary.getSubNode();  // groupby
    ExprType curType = unaryChild.getType();
    if (unaryChild.getSubNode().getType() == ExprType.STORE) {
      // store - groupby - store
      unaryChild = (UnaryNode) unaryChild.getSubNode(); // store
      prevStore = (StoreTableNode) unaryChild;
      newScan = GlobalPlannerUtils.newScanPlan(prevStore.getOutSchema(),
          prevStore.getTableName(), sm.getTablePath(prevStore.getTableName()));
      newScan.setLocal(true);
      ((UnaryNode) unary.getSubNode()).setSubNode(newScan);
      prev = convertMap.get(prevStore);
      if (prev != null) {
        prev.setParentQuery(unit);
        unit.addChildQuery(newScan, prev);
        if (unaryChild.getSubNode().getType() == curType) {
          // TODO - this is duplicated code
          prev.setOutputType(PARTITION_TYPE.RANGE);
        } else {
          prev.setOutputType(PARTITION_TYPE.LIST);
        }
      }
      if (unaryChild.getSubNode().getType() == curType) {
        // the second phase
        unit.setOutputType(PARTITION_TYPE.LIST);
      } else {
        // the first phase
        unit.setOutputType(PARTITION_TYPE.HASH);
      }
    } else if (unaryChild.getSubNode().getType() == ExprType.SCAN) {
      // the first phase
      // store - sort - scan
      unit.setOutputType(PARTITION_TYPE.RANGE);
    } else if (unaryChild.getSubNode().getType() == ExprType.UNION) {
      _handleUnionNode(rootStore, (UnionNode)unaryChild.getSubNode(), unit,
          null, PARTITION_TYPE.LIST);
    } else {
      // error
    }
    return unit;
  }
  
  private SubQuery makeJoinSubQuery(StoreTableNode rootStore,
                                    LogicalNode plan, SubQuery unit) throws IOException {
    UnaryNode unary = (UnaryNode)plan;
    StoreTableNode outerStore, innerStore;
    SubQuery prev;
    JoinNode join = (JoinNode) unary.getSubNode();
    Schema outerSchema = join.getOuterNode().getOutSchema();
    Schema innerSchema = join.getInnerNode().getOutSchema();
    unit.setOutputType(PARTITION_TYPE.LIST);

    List<Column> outerCollist = new ArrayList<>();
    List<Column> innerCollist = new ArrayList<>();
    
    // TODO: set partition for store nodes
    if (join.hasJoinQual()) {
      // getting repartition keys
      List<Column[]> cols = PlannerUtil.getJoinKeyPairs(join.getJoinQual(), outerSchema, innerSchema);
      for (Column [] pair : cols) {
        outerCollist.add(pair[0]);
        innerCollist.add(pair[1]);
      }
    } else {
      // broadcast
    }
    
    Column[] outerCols = new Column[outerCollist.size()];
    Column[] innerCols = new Column[innerCollist.size()];
    outerCols = outerCollist.toArray(outerCols);
    innerCols = innerCollist.toArray(innerCols);
    
    // outer
    if (join.getOuterNode().getType() == ExprType.STORE) {
      outerStore = (StoreTableNode) join.getOuterNode();
      TableMeta outerMeta = TCatUtil.newTableMeta(outerStore.getOutSchema(),
          StoreType.CSV);
      insertOuterScan(join, outerStore.getTableName(), outerMeta);
      prev = convertMap.get(outerStore);
      if (prev != null) {
        prev.setOutputType(PARTITION_TYPE.HASH);
        prev.setParentQuery(unit);
        unit.addChildQuery((ScanNode)join.getOuterNode(), prev);
      }
      outerStore.setPartitions(PARTITION_TYPE.HASH, outerCols, 32);
    } else if (join.getOuterNode().getType() == ExprType.UNION) {
      _handleUnionNode(rootStore, (UnionNode)join.getOuterNode(), unit, 
          outerCols, PARTITION_TYPE.HASH);
    } else {

    }
    
    // inner
    if (join.getInnerNode().getType() == ExprType.STORE) {
      innerStore = (StoreTableNode) join.getInnerNode();
      TableMeta innerMeta = TCatUtil.newTableMeta(innerStore.getOutSchema(),
          StoreType.CSV);
      insertInnerScan(join, innerStore.getTableName(), innerMeta);
      prev = convertMap.get(innerStore);
      if (prev != null) {
        prev.setOutputType(PARTITION_TYPE.HASH);
        prev.setParentQuery(unit);
        unit.addChildQuery((ScanNode)join.getInnerNode(), prev);
      }
      innerStore.setPartitions(PARTITION_TYPE.HASH, innerCols, 32);
    } else if (join.getInnerNode().getType() == ExprType.UNION) {
      _handleUnionNode(rootStore, (UnionNode)join.getInnerNode(), unit,
          innerCols, PARTITION_TYPE.HASH);
    }
    
    return unit;
  }
  
  /**
   * Recursive하게 union의 자식 plan들을 설정
   * 
   * @param rootStore 생성할 SubQuery의 store
   * @param union union을 root로 하는 logical plan
   * @param cur 생성할 SubQuery
   * @param cols partition 정보를 설정하기 위한 column array
   * @param prevOutputType 자식 SubQuery의 partition type
   * @throws IOException
   */
  private void _handleUnionNode(StoreTableNode rootStore, UnionNode union, 
      SubQuery cur, Column[] cols, PARTITION_TYPE prevOutputType)
          throws IOException {
    StoreTableNode store;
    TableMeta meta;
    SubQuery prev;
    
    if (union.getOuterNode().getType() == ExprType.STORE) {
      store = (StoreTableNode) union.getOuterNode();
      meta = TCatUtil.newTableMeta(store.getOutSchema(), StoreType.CSV);
      insertOuterScan(union, store.getTableName(), meta);
      prev = convertMap.get(store);
      if (prev != null) {
        prev.getStoreTableNode().setTableName(rootStore.getTableName());
        prev.setOutputType(prevOutputType);
        prev.setParentQuery(cur);
        cur.addChildQuery((ScanNode)union.getOuterNode(), prev);
      }
      if (cols != null) {
        store.setPartitions(PARTITION_TYPE.LIST, cols, 32);
      }
    } else if (union.getOuterNode().getType() == ExprType.UNION) {
      _handleUnionNode(rootStore, (UnionNode)union.getOuterNode(), cur, cols, 
          prevOutputType);
    }
    
    if (union.getInnerNode().getType() == ExprType.STORE) {
      store = (StoreTableNode) union.getInnerNode();
      meta = TCatUtil.newTableMeta(store.getOutSchema(), StoreType.CSV);
      insertInnerScan(union, store.getTableName(), meta);
      prev = convertMap.get(store);
      if (prev != null) {
        prev.getStoreTableNode().setTableName(rootStore.getTableName());
        prev.setOutputType(prevOutputType);
        prev.setParentQuery(cur);
        cur.addChildQuery((ScanNode)union.getInnerNode(), prev);
      }
      if (cols != null) {
        store.setPartitions(PARTITION_TYPE.LIST, cols, 32);
      }
    } else if (union.getInnerNode().getType() == ExprType.UNION) {
      _handleUnionNode(rootStore, (UnionNode)union.getInnerNode(), cur, cols, 
          prevOutputType);
    }
  }

  @VisibleForTesting
  public SubQuery createMultilevelGroupby(
      SubQuery firstPhaseGroupby, Column[] keys)
      throws CloneNotSupportedException, IOException {
    SubQuery secondPhaseGroupby = firstPhaseGroupby.getParentQuery();
    Preconditions.checkState(secondPhaseGroupby.getScanNodes().length == 1);

    ScanNode secondScan = secondPhaseGroupby.getScanNodes()[0];
    GroupbyNode secondGroupby = (GroupbyNode) secondPhaseGroupby.
        getStoreTableNode().getSubNode();
    SubQuery newPhaseGroupby = new SubQuery(
        QueryIdFactory.newSubQueryId(
            firstPhaseGroupby.getId().getQueryId()), sm, this);
    LogicalNode tmp = PlannerUtil.findTopParentNode(
        firstPhaseGroupby.getLogicalPlan(), ExprType.GROUP_BY);
    GroupbyNode firstGroupby;
    if (tmp instanceof UnaryNode) {
      firstGroupby = (GroupbyNode) ((UnaryNode)tmp).getSubNode();
      GroupbyNode newFirstGroupby = GlobalPlannerUtils.newGroupbyPlan(
          firstGroupby.getInSchema(),
          firstGroupby.getOutSchema(),
          keys,
          firstGroupby.getHavingCondition(),
          firstGroupby.getTargets()
      );
      newFirstGroupby.setSubNode(firstGroupby.getSubNode());
      ((UnaryNode) tmp).setSubNode(newFirstGroupby);
    }

    // create a new SubQuery containing the group by plan
    StoreTableNode newStore = GlobalPlannerUtils.newStorePlan(
        secondScan.getInSchema(),
        newPhaseGroupby.getId().toString());
    newStore.setLocal(true);
    ScanNode newScan = GlobalPlannerUtils.newScanPlan(
        firstPhaseGroupby.getOutputSchema(),
        firstPhaseGroupby.getOutputName(),
        sm.getTablePath(firstPhaseGroupby.getOutputName()));
    newScan.setLocal(true);
    GroupbyNode newGroupby = GlobalPlannerUtils.newGroupbyPlan(
        newScan.getOutSchema(),
        newStore.getInSchema(),
        keys,
        secondGroupby.getHavingCondition(),
        secondGroupby.getTargets());
    newGroupby.setSubNode(newScan);
    newStore.setSubNode(newGroupby);
    newPhaseGroupby.setLogicalPlan(newStore);

    secondPhaseGroupby.removeChildQuery(secondScan);

    // update the scan node of last phase
    secondScan = GlobalPlannerUtils.newScanPlan(secondScan.getInSchema(),
        newPhaseGroupby.getOutputName(),
        sm.getTablePath(newPhaseGroupby.getOutputName()));
    secondScan.setLocal(true);
    secondGroupby.setSubNode(secondScan);
    secondPhaseGroupby.setLogicalPlan(secondPhaseGroupby.getLogicalPlan());

    // insert the new SubQuery
    // between the first phase and the second phase
    secondPhaseGroupby.addChildQuery(secondScan, newPhaseGroupby);
    newPhaseGroupby.addChildQuery(newPhaseGroupby.getScanNodes()[0],
        firstPhaseGroupby);
    newPhaseGroupby.setParentQuery(secondPhaseGroupby);
    firstPhaseGroupby.setParentQuery(newPhaseGroupby);

    return newPhaseGroupby;
  }
  
  private LogicalNode insertOuterScan(BinaryNode parent, String tableId,
      TableMeta meta) throws IOException {
    TableDesc desc = TCatUtil.newTableDesc(tableId, meta, sm.getTablePath(tableId));
    ScanNode scan = new ScanNode(new FromTable(desc));
    scan.setLocal(true);
    scan.setInSchema(meta.getSchema());
    scan.setOutSchema(meta.getSchema());
    parent.setOuter(scan);
    return parent;
  }
  
  private LogicalNode insertInnerScan(BinaryNode parent, String tableId, 
      TableMeta meta) throws IOException {
    TableDesc desc = TCatUtil.newTableDesc(tableId, meta, sm.getTablePath(tableId));
    ScanNode scan = new ScanNode(new FromTable(desc));
    scan.setLocal(true);
    scan.setInSchema(meta.getSchema());
    scan.setOutSchema(meta.getSchema());
    parent.setInner(scan);
    return parent;
  }
  
  private MasterPlan convertToGlobalPlan(IndexWriteNode index,
                                         LogicalNode logicalPlan) throws IOException {
    recursiveBuildSubQuery(logicalPlan);
    SubQuery root;
    
    if (index != null) {
      SubQueryId id = QueryIdFactory.newSubQueryId(queryId);
      SubQuery unit = new SubQuery(id, sm, this);
      root = makeScanSubQuery(unit);
      root.setLogicalPlan(index);
    } else {
      root = convertMap.get(((LogicalRootNode)logicalPlan).getSubNode());
      root.getStoreTableNode().setLocal(false);
    }
    return new MasterPlan(root);
  }
  
  public SubQuery setPartitionNumberForTwoPhase(SubQuery subQuery, final int n) {
    Column[] keys = null;
    // if the next query is join,
    // set the partition number for the current logicalUnit
    // TODO: the union handling is required when a join has unions as its child
    SubQuery parentQueryUnit = subQuery.getParentQuery();
    if (parentQueryUnit != null) {
      if (parentQueryUnit.getStoreTableNode().getSubNode().getType() == ExprType.JOIN) {
        subQuery.getStoreTableNode().setPartitions(subQuery.getOutputType(),
            subQuery.getStoreTableNode().getPartitionKeys(), n);
        keys = subQuery.getStoreTableNode().getPartitionKeys();
      }
    }

    StoreTableNode store = subQuery.getStoreTableNode();
    // set the partition number for group by and sort
    if (subQuery.getOutputType() == PARTITION_TYPE.HASH) {
      if (store.getSubNode().getType() == ExprType.GROUP_BY) {
        GroupbyNode groupby = (GroupbyNode)store.getSubNode();
        keys = groupby.getGroupingColumns();
      }
    } else if (subQuery.getOutputType() == PARTITION_TYPE.RANGE) {
      if (store.getSubNode().getType() == ExprType.SORT) {
        SortNode sort = (SortNode)store.getSubNode();
        keys = new Column[sort.getSortKeys().length];
        for (int i = 0; i < keys.length; i++) {
          keys[i] = sort.getSortKeys()[i].getSortKey();
        }
      }
    }
    if (keys != null) {
      if (keys.length == 0) {
        store.setPartitions(subQuery.getOutputType(), new Column[]{}, 1);
      } else {
        store.setPartitions(subQuery.getOutputType(), keys, n);
      }
    } else {
      store.setListPartition();
    }
    return subQuery;
  }

  /**
   * 입력 받은 SubQuery를 QueryUnit들로 localize
   * 
   * @param subQuery localize할 SubQuery
   * @param maxTaskNum localize된 QueryUnit의 최대 개수
   * @return
   * @throws IOException
   * @throws URISyntaxException
   */
  public QueryUnit[] localize(SubQuery subQuery, int maxTaskNum)
      throws IOException, URISyntaxException {
    FileStatus[] files;
    Fragment[] frags;
    List<Fragment> fragList;
    List<URI> uriList;
    // fragments and fetches are maintained for each scan of the SubQuery
    Map<ScanNode, List<Fragment>> fragMap = new HashMap<>();
    Map<ScanNode, List<URI>> fetchMap = new HashMap<>();
    
    // set partition numbers for two phase algorithms
    // TODO: the following line might occur a bug. see the line 623
    subQuery = setPartitionNumberForTwoPhase(subQuery, maxTaskNum);

    SortSpec [] sortSpecs = null;
    Schema sortSchema;
    
    // make fetches and fragments for each scan
    Path tablepath;
    ScanNode[] scans = subQuery.getScanNodes();
    for (ScanNode scan : scans) {
      if (scan.getTableId().startsWith(QueryId.PREFIX)) {
        tablepath = sm.getTablePath(scan.getTableId());
      } else {
        tablepath = catalog.getTableDesc(scan.getTableId()).getPath();
      }
      if (scan.isLocal()) {
        SubQuery prev = subQuery.getChildIterator().next();
        TableStat stat = prev.getStats();
        if (stat.getNumRows() == 0) {
          return new QueryUnit[0];
        }
        // make fetches from the previous query units
        uriList = new ArrayList<>();
        fragList = new ArrayList<>();

        if (prev.getOutputType() == PARTITION_TYPE.RANGE) {
          StoreTableNode store = (StoreTableNode) prev.getLogicalPlan();
          SortNode sort = (SortNode) store.getSubNode();
          sortSpecs = sort.getSortKeys();
          sortSchema = PlannerUtil.sortSpecsToSchema(sort.getSortKeys());

          // calculate the number of tasks based on the data size
          int mb = (int) Math.ceil((double)stat.getNumBytes() / 1048576);
          LOG.info("Total size of intermediate data is approximately " + mb + " MB");

          maxTaskNum = (int) Math.ceil((double)mb / 64); // determine the number of task by considering 1 task per 64MB
          LOG.info("The desired number of tasks is set to " + maxTaskNum);

          // calculate the number of maximum query ranges
          TupleRange mergedRange =
              TupleUtil.columnStatToRange(sort.getOutSchema(),
                  sortSchema, stat.getColumnStats());
          RangePartitionAlgorithm partitioner =
              new UniformRangePartition(sortSchema, mergedRange);
          BigDecimal card = partitioner.getTotalCardinality();

          // if the number of the range cardinality is less than the desired number of tasks,
          // we set the the number of tasks to the number of range cardinality.
          if (card.compareTo(new BigDecimal(maxTaskNum)) < 0) {
            LOG.info("The range cardinality (" + card
                + ") is less then the desired number of tasks (" + maxTaskNum + ")");
            maxTaskNum = card.intValue();
          }

          LOG.info("Try to divide " + mergedRange + " into " + maxTaskNum +
              " sub ranges (total units: " + maxTaskNum + ")");
          TupleRange [] ranges = partitioner.partition(maxTaskNum);

          String [] queries = TupleUtil.rangesToQueries(sortSpecs, ranges);
          for (QueryUnit qu : subQuery.getChildQuery(scan).getQueryUnits()) {
            for (Partition p : qu.getPartitions()) {
              for (String query : queries) {
                uriList.add(new URI(p.getFileName() + "&" + query));
              }
            }
          }
        } else {
          SubQuery child = subQuery.getChildQuery(scan);
          QueryUnit[] units;
          if (child.getStoreTableNode().getSubNode().getType() ==
              ExprType.UNION) {
            List<QueryUnit> list = Lists.newArrayList();
            for (ScanNode s : child.getScanNodes()) {
              for (QueryUnit qu : child.getChildQuery(s).getQueryUnits()) {
                list.add(qu);
              }
            }
            units = new QueryUnit[list.size()];
            units = list.toArray(units);
          } else {
            units = child.getQueryUnits();
          }
          for (QueryUnit qu : units) {
            for (Partition p : qu.getPartitions()) {
              uriList.add(new URI(p.getFileName()));
//              System.out.println("Partition: " + uriList.get(uriList.size() - 1));
            }
          }
        }
        
        fetchMap.put(scan, uriList);
        // one fragment is shared by query units
        Fragment frag = new Fragment(scan.getTableId(), tablepath,
            TCatUtil.newTableMeta(scan.getInSchema(),StoreType.CSV),
            0, 0, null);
        fragList.add(frag);
        fragMap.put(scan, fragList);
      } else {
        fragList = new ArrayList<>();
        // set fragments for each scan
        if (subQuery.hasChildQuery() &&
            (subQuery.getChildQuery(scan).getOutputType() == PARTITION_TYPE.HASH ||
            subQuery.getChildQuery(scan).getOutputType() == PARTITION_TYPE.RANGE)
            ) {
          files = sm.getFileSystem().listStatus(tablepath);
        } else {
          files = new FileStatus[1];
          files[0] = sm.getFileSystem().getFileStatus(tablepath);
        }
        for (FileStatus file : files) {
          // make fragments
          if (scan.isBroadcast()) {
            frags = sm.splitBroadcastTable(file.getPath());
          } else {
            frags = sm.split(file.getPath());
          }
          for (Fragment f : frags) {
            // TODO: the fragment ID should be set before
            f.setId(scan.getTableId());
            fragList.add(f);
          }
        }
        fragMap.put(scan, fragList);
      }
    }

    List<QueryUnit> unitList = null;
    if (scans.length == 1) {
      unitList = makeUnaryQueryUnit(subQuery, maxTaskNum, fragMap, fetchMap,
          sortSpecs);
    } else if (scans.length == 2) {
      unitList = makeBinaryQueryUnit(subQuery, maxTaskNum, fragMap, fetchMap);
    }
    // TODO: The partition number should be set here,
    // because the number of query units is decided above.

    QueryUnit[] units = new QueryUnit[unitList.size()];
    units = unitList.toArray(units);
    subQuery.setQueryUnits(unitList);
    
    return units;
  }
  
  /**
   * 2개의 scan을 가진 QueryUnit들에 fragment와 fetch를 할당
   * 
   * @param subQuery
   * @param n
   * @param fragMap
   * @param fetchMap
   * @return
   */
  private List<QueryUnit> makeBinaryQueryUnit(SubQuery subQuery, final int n,
      Map<ScanNode, List<Fragment>> fragMap, 
      Map<ScanNode, List<URI>> fetchMap) {
    List<QueryUnit> queryUnits = new ArrayList<>();
    final int maxQueryUnitNum = n;
    ScanNode[] scans = subQuery.getScanNodes();
    
    if (subQuery.hasChildQuery()) {
      SubQuery prev = subQuery.getChildQuery(scans[0]);
      switch (prev.getOutputType()) {
      case BROADCAST:
        throw new NotImplementedException();
      case HASH:
        if (scans[0].isLocal()) {
          queryUnits = assignFetchesToBinaryByHash(subQuery, queryUnits,
              fetchMap, maxQueryUnitNum);
          queryUnits = assignEqualFragment(queryUnits, fragMap);
        } else {
          throw new NotImplementedException();
        }
        break;
      case LIST:
        throw new NotImplementedException();
      }
    } else {
//      unitList = assignFragmentsByRoundRobin(unit, unitList, fragMap,
//          maxQueryUnitNum);
      queryUnits = makeQueryUnitsForBinaryPlan(subQuery,
          queryUnits, fragMap);
    }

    return queryUnits;
  }

  public List<QueryUnit> makeQueryUnitsForBinaryPlan(
      SubQuery subQuery, List<QueryUnit> queryUnits,
      Map<ScanNode, List<Fragment>> fragmentMap) {
    QueryUnit queryUnit;
    if (subQuery.hasJoinPlan()) {
      // make query units for every composition of fragments of each scan
      Preconditions.checkArgument(fragmentMap.size()==2);

      ScanNode [] scanNodes = subQuery.getScanNodes();
      String innerId = null, outerId = null;

      // If one relation is set to broadcast, it meaning that the relation
      // is less than one block size. That is, the relation has only
      // one fragment. If this assumption is kept, the below code is always
      // correct.
      if (scanNodes[0].isBroadcast() || scanNodes[1].isBroadcast()) {
        List<Fragment> broadcastFrag = null;
        List<Fragment> baseFrag = null;
        if (scanNodes[0].isBroadcast()) {
          broadcastFrag = fragmentMap.get(scanNodes[0]);
          baseFrag = fragmentMap.get(scanNodes[1]);

          innerId = scanNodes[0].getTableId();
          outerId = scanNodes[1].getTableId();
        } else if (scanNodes[1].isBroadcast()) {
          broadcastFrag = fragmentMap.get(scanNodes[1]);
          baseFrag = fragmentMap.get(scanNodes[0]);

          innerId = scanNodes[1].getTableId();
          outerId = scanNodes[0].getTableId();
        }

        for (Fragment outer : baseFrag) {
          queryUnit = newQueryUnit(subQuery);
          queryUnit.setFragment(outerId, outer);
          for (Fragment inner : broadcastFrag) {
            queryUnit.setFragment(innerId, inner);
          }
          queryUnits.add(queryUnit);
        }
      } else {
        List<Fragment> innerFrags, outerFrags;
        Iterator<Entry<ScanNode, List<Fragment>>> it =
            fragmentMap.entrySet().iterator();
        Entry<ScanNode, List<Fragment>> e = it.next();
        innerId = e.getKey().getTableId();
        innerFrags = e.getValue();
        e = it.next();
        outerId = e.getKey().getTableId();
        outerFrags = e.getValue();
        for (Fragment outer : outerFrags) {
          for (Fragment inner : innerFrags) {
            queryUnit = newQueryUnit(subQuery);
            queryUnit.setFragment(innerId, inner);
            queryUnit.setFragment(outerId, outer);
            queryUnits.add(queryUnit);
          }
        }
      }
    }

    return queryUnits;
  }
  
  /**
   * 1개의 scan을 가진 QueryUnit들에 대해 fragment와 fetch를 할당
   * 
   * @param subQuery
   * @param n
   * @param fragMap
   * @param fetchMap
   * @return
   */
  private List<QueryUnit> makeUnaryQueryUnit(SubQuery subQuery, int n,
      Map<ScanNode, List<Fragment>> fragMap, 
      Map<ScanNode, List<URI>> fetchMap, SortSpec[] sortSpecs) throws UnsupportedEncodingException {
    List<QueryUnit> queryUnits = new ArrayList<>();
    int maxQueryUnitNum;
    ScanNode scan = subQuery.getScanNodes()[0];
    maxQueryUnitNum = n;
    if (subQuery.hasChildQuery()) {
      SubQuery child = subQuery.getChildQuery(scan);
      if (child.getStoreTableNode().getSubNode().getType() ==
          ExprType.GROUP_BY) {
        GroupbyNode groupby = (GroupbyNode) child.getStoreTableNode().
            getSubNode();
        if (groupby.getGroupingColumns().length == 0) {
          maxQueryUnitNum = 1;
        }
      }
      switch (child.getOutputType()) {
        case BROADCAST:
        throw new NotImplementedException();

        case HASH:
          if (scan.isLocal()) {
            queryUnits = assignFetchesToUnaryByHash(subQuery,
                queryUnits, scan, fetchMap.get(scan), maxQueryUnitNum);
            queryUnits = assignEqualFragment(queryUnits, scan,
                fragMap.get(scan).get(0));
          } else {
            throw new NotImplementedException();
          }
          break;
        case RANGE:
          if (scan.isLocal()) {
            Schema rangeSchema = PlannerUtil.sortSpecsToSchema(sortSpecs);
            queryUnits = assignFetchesByRange(subQuery,
                queryUnits, scan, fetchMap.get(scan),
                maxQueryUnitNum, rangeSchema, sortSpecs[0].isAscending());
            queryUnits = assignEqualFragment(queryUnits, scan,
                fragMap.get(scan).get(0));
          } else {
            throw new NotImplementedException();
          }
          break;

        case LIST:
          if (scan.isLocal()) {
            queryUnits = assignFetchesByRoundRobin(subQuery,
                queryUnits, scan, fetchMap.get(scan), maxQueryUnitNum);
            queryUnits = assignEqualFragment(queryUnits, scan,
                fragMap.get(scan).get(0));
          } else {
            throw new NotImplementedException();
          }
          break;
      }
    } else {
//      queryUnits = assignFragmentsByRoundRobin(subQuery, queryUnits, scan,
//          fragMap.get(scan), maxQueryUnitNum);
      queryUnits = makeQueryUnitsForEachFragment(subQuery,
          queryUnits, scan, fragMap.get(scan));
    }
    return queryUnits;
  }

  private List<QueryUnit> makeQueryUnitsForEachFragment(
      SubQuery subQuery, List<QueryUnit> queryUnits,
      ScanNode scan, List<Fragment> fragments) {
    QueryUnit queryUnit;
    for (Fragment fragment : fragments) {
      queryUnit = newQueryUnit(subQuery);
      queryUnit.setFragment(scan.getTableId(), fragment);
      queryUnits.add(queryUnit);
    }
    return queryUnits;
  }
  
  private QueryUnit newQueryUnit(SubQuery subQuery) {
    QueryUnit unit = new QueryUnit(
        QueryIdFactory.newQueryUnitId(subQuery.getId()), subQuery.isLeafQuery(),
        subQuery.eventHandler);
    unit.setLogicalPlan(subQuery.getLogicalPlan());
    return unit;
  }

  private QueryUnit newQueryUnit(SubQuery subQuery, int taskId) {
    QueryUnit unit = new QueryUnit(
        QueryIdFactory.newQueryUnitId(subQuery.getId(), taskId), subQuery.isLeafQuery(),
        subQuery.eventHandler);
    unit.setLogicalPlan(subQuery.getLogicalPlan());
    return unit;
  }
  
  /**
   * Binary QueryUnit들에 hash 파티션된 fetch를 할당
   * 
   * @param subQuery
   * @param unitList
   * @param fetchMap
   * @param n
   * @return
   */
  private List<QueryUnit> assignFetchesToBinaryByHash(SubQuery subQuery,
      List<QueryUnit> unitList, Map<ScanNode, List<URI>> fetchMap, final int n) {
    QueryUnit unit;
    int i = 0;
    Map<String, Map<ScanNode, List<URI>>> hashed = hashFetches(fetchMap);
    Iterator<Entry<String, Map<ScanNode, List<URI>>>> it = 
        hashed.entrySet().iterator();
    Entry<String, Map<ScanNode, List<URI>>> e;
    while (it.hasNext()) {
      e = it.next();
      if (e.getValue().size() == 2) { // only if two matched partitions
        if (unitList.size() == n) {
          unit = unitList.get(i++);
          if (i == unitList.size()) {
            i = 0;
          }
        } else {
          unit = newQueryUnit(subQuery);
          unitList.add(unit);
        }
        Map<ScanNode, List<URI>> m = e.getValue();
        for (ScanNode scan : m.keySet()) {
          for (URI uri : m.get(scan)) {
            unit.addFetch(scan.getTableId(), uri);
          }
        }
      }
    }
    
    return unitList;
  }
  
  /**
   * Unary QueryUnit들에 hash 파티션된 fetch를 할당
   * 
   * @param subQuery
   * @param unitList
   * @param scan
   * @param uriList
   * @param n
   * @return
   */
  private List<QueryUnit> assignFetchesToUnaryByHash(SubQuery subQuery,
      List<QueryUnit> unitList, ScanNode scan, List<URI> uriList, int n) {
    Map<String, List<URI>> hashed = hashFetches(subQuery.getId(), uriList); // hash key, uri
    QueryUnit unit;
    int i = 0;
    // TODO: units에 hashed 할당
    Iterator<Entry<String, List<URI>>> it = hashed.entrySet().iterator();
    Entry<String, List<URI>> e;
    while (it.hasNext()) {
      e = it.next();
      if (unitList.size() == n) {
        unit = unitList.get(i++);
        if (i == unitList.size()) {
          i = 0;
        }
      } else {
        unit = newQueryUnit(subQuery);
        unitList.add(unit);
      }
      unit.addFetches(scan.getTableId(), e.getValue());
    }
    return unitList;
  }

  private List<QueryUnit> assignFetchesByRange(SubQuery subQuery,
                                              List<QueryUnit> unitList,
                                              ScanNode scan,
                                              List<URI> uriList,
                                              int n,
                                              Schema rangeSchema,
                                              boolean ascendingFirstKey)
      throws UnsupportedEncodingException {
    Map<TupleRange, Set<URI>> partitions =
        rangeFetches(rangeSchema, uriList, ascendingFirstKey);
    // grouping urls by range
    QueryUnit unit;
    int i = 0;
    Iterator<Entry<TupleRange, Set<URI>>> it = partitions.entrySet().iterator();
    Entry<TupleRange, Set<URI>> e;
    while (it.hasNext()) {
      e = it.next();
      if (unitList.size() == n) {
        unit = unitList.get(i++);
        if (i == unitList.size()) {
          i = 0;
        }
      } else {
        unit = newQueryUnit(subQuery);
        unitList.add(unit);
      }
      unit.addFetches(scan.getTableId(), e.getValue());
    }
    return unitList;
  }
  
  /**
   * Unary QueryUnit들에 list 파티션된 fetch를 할당
   * 
   * @param subQuery
   * @param unitList
   * @param scan
   * @param uriList
   * @param n
   * @return
   */
  private List<QueryUnit> assignFetchesByRoundRobin(SubQuery subQuery,
      List<QueryUnit> unitList, ScanNode scan, List<URI> uriList, int n) { 
    QueryUnit unit;
    int i = 0;
    for (URI uri : uriList) {
      if (unitList.size() < n) {
        unit = newQueryUnit(subQuery);
        unitList.add(unit);
      } else {
        unit = unitList.get(i++);
        if (i == unitList.size()) {
          i = 0;
        }
      }
      unit.addFetch(scan.getTableId(), uri);
    }
    return unitList;
  }

  @VisibleForTesting
  public static Map<String, Map<ScanNode, List<URI>>> hashFetches(Map<ScanNode, List<URI>> uriMap) {
    SortedMap<String, Map<ScanNode, List<URI>>> hashed = new TreeMap<>();
    String uriPath, key;
    Map<ScanNode, List<URI>> m;
    List<URI> uriList;
    for (Entry<ScanNode, List<URI>> e : uriMap.entrySet()) {
      for (URI uri : e.getValue()) {
        uriPath = uri.toString();
        key = uriPath.substring(uriPath.lastIndexOf("=")+1);
        if (hashed.containsKey(key)) {
          m = hashed.get(key);
        } else {
          m = new HashMap<>();
        }
        if (m.containsKey(e.getKey())) {
          uriList = m.get(e.getKey());
        } else {
          uriList = new ArrayList<>();
        }
        uriList.add(uri);
        m.put(e.getKey(), uriList);
        hashed.put(key, m);
      }
    }

    SortedMap<String, Map<ScanNode, List<URI>>> finalHashed = new TreeMap<String, Map<ScanNode,List<URI>>>();
    for (Entry<String, Map<ScanNode, List<URI>>> entry : hashed.entrySet()) {
      finalHashed.put(entry.getKey(), combineURIByHostForBinary(entry.getValue()));
    }
    
    return finalHashed;
  }

  private static Map<ScanNode, List<URI>> combineURIByHostForBinary(Map<ScanNode, List<URI>> hashed) {
    Map<ScanNode, List<URI>> finalHashed = Maps.newHashMap();
    for (Entry<ScanNode, List<URI>> urisByKey : hashed.entrySet()) {
      Map<String, List<String>> param = new QueryStringDecoder(urisByKey.getValue().get(0)).getParameters();
      QueryUnitAttemptId quid = new QueryUnitAttemptId(
          new QueryStringDecoder(urisByKey.getValue().get(0)).getParameters().get("qid").get(0));
      SubQueryId sid = quid.getSubQueryId();
      String fn = param.get("fn").get(0);
      Map<String, List<String>> quidByHost = Maps.newHashMap();
      for(URI uri : urisByKey.getValue()) {
        final Map<String,List<String>> params =
            new QueryStringDecoder(uri).getParameters();
        if (quidByHost.containsKey(uri.getHost() + ":" + uri.getPort())) {
          quidByHost.get(uri.getHost() + ":" + uri.getPort()).add(params.get("qid").get(0));
        } else {
          quidByHost.put(uri.getHost() + ":" + uri.getPort(), Lists.newArrayList(params.get("qid")));
        }
      }

      finalHashed.put(urisByKey.getKey(), mergeURI(quidByHost, sid.toString(), fn));
    }

    return finalHashed;
  }


  @VisibleForTesting
  public static Map<String, List<URI>> hashFetches(SubQueryId sid, List<URI> uriList) {
    SortedMap<String, List<URI>> hashed = new TreeMap<>();
    String uriPath, key;
    for (URI uri : uriList) {
      // TODO
      uriPath = uri.toString();
      key = uriPath.substring(uriPath.lastIndexOf("=")+1);
      if (hashed.containsKey(key)) {
        hashed.get(key).add(uri);
      } else {
        List<URI> list = new ArrayList<>();
        list.add(uri);
        hashed.put(key, list);
      }
    }
    
    return combineURIByHost(hashed);
  }

  private static Map<String, List<URI>> combineURIByHost(Map<String, List<URI>> hashed) {
    Map<String, List<URI>> finalHashed = Maps.newTreeMap();
    for (Entry<String, List<URI>> urisByKey : hashed.entrySet()) {
      QueryUnitAttemptId quid = new QueryUnitAttemptId(
          new QueryStringDecoder(urisByKey.getValue().get(0)).getParameters().get("qid").get(0));
      SubQueryId sid = quid.getSubQueryId();
      Map<String,List<String>> quidByHost = Maps.newHashMap();
      for(URI uri : urisByKey.getValue()) {
        final Map<String,List<String>> params =
            new QueryStringDecoder(uri).getParameters();
        if (quidByHost.containsKey(uri.getHost() + ":" + uri.getPort())) {
          quidByHost.get(uri.getHost() + ":" + uri.getPort()).add(params.get("qid").get(0));
        } else {
          quidByHost.put(uri.getHost() + ":" + uri.getPort(), Lists.newArrayList(params.get("qid")));
        }
      }
      finalHashed.put(urisByKey.getKey(), mergeURI(quidByHost, sid.toString(), urisByKey.getKey()));
    }
    return finalHashed;
  }

  private static List<URI> mergeURI(Map<String, List<String>> quidByKey, String sid, String fn) {
    List<URI> uris = Lists.newArrayList();
    for (Entry<String, List<String>> quidByHost : quidByKey.entrySet()) {
      StringBuilder sb = new StringBuilder("http://")
          .append(quidByHost.getKey()).append("/").append("?fn="+fn).append("&sid="+sid);
      sb.append("&qid=");
      boolean first = true;
      for (String qid : quidByHost.getValue()) {
        if (first) {
          first = false;
        } else {
          sb.append(",");
        }

        QueryUnitAttemptId quid = new QueryUnitAttemptId(qid);
        sb.append(quid.getQueryUnitId().getId() + "_");
        sb.append(quid.getId());
      }
      uris.add(URI.create(sb.toString()));
    }
    return uris;
  }

  private Map<TupleRange, Set<URI>> rangeFetches(final Schema schema,
                                                 final List<URI> uriList,
                                                 final boolean ascendingFirstKey)
      throws UnsupportedEncodingException {
    SortedMap<TupleRange, Set<URI>> map;
    if (ascendingFirstKey) {
      map = new TreeMap<>();
    } else {
      map = new TreeMap<>(new TupleRange.DescendingTupleRangeComparator());
    }
    TupleRange range;
    Set<URI> uris;
    for (URI uri : uriList) {
      // URI.getQuery() returns a url-decoded query string.
      range = TupleUtil.queryToRange(schema, uri.getQuery());
      if (map.containsKey(range)) {
        uris = map.get(range);
        uris.add(uri);
      } else {
        uris = Sets.newHashSet();
        uris.add(uri);
        map.put(range, uris);
      }
    }

    return map;
  }

  /**
   * Unary QueryUnit들에 broadcast partition된 fetch를 할당
   *
   * @param units
   * @param scan
   * @param uriList
   */
  private void assignFetchesByBroadcast(QueryUnit[] units, ScanNode scan, List<URI> uriList) {
    for (URI uri : uriList) {
      for (QueryUnit unit : units) {
        // TODO: add each uri to every units
        unit.addFetch(scan.getTableId(), uri);
      }
    }
  }

  /**
   * Unary QueryUnit들에 대하여 동일한 fragment를 할당
   * 
   * @param unitList
   * @param scan
   * @param frag
   * @return
   */
  private List<QueryUnit> assignEqualFragment(List<QueryUnit> unitList, 
      ScanNode scan, Fragment frag) {
    for (int i = 0; i < unitList.size(); i++) {
      unitList.get(i).setFragment(scan.getTableId(), frag);
    }
    
    return unitList;
  }
  
  /**
   * Binary QueryUnit들에 대하여 scan별로 동일한 fragment를 할당
   * @param unitList
   * @param fragMap
   * @return
   */
  private List<QueryUnit> assignEqualFragment(List<QueryUnit> unitList, 
      Map<ScanNode, List<Fragment>> fragMap) {
    for (int i = 0; i < unitList.size(); i++) {
      for (ScanNode scan : fragMap.keySet()) {
        unitList.get(i).setFragment(scan.getTableId(),
            fragMap.get(scan).get(0));
      }
    }
    return unitList;
  }
  
  private Map<String, Map<ScanNode, List<Fragment>>> hashFragments(Map<ScanNode, 
      List<Fragment>> fragMap) {
    SortedMap<String, Map<ScanNode, List<Fragment>>> hashed = 
        new TreeMap<String, Map<ScanNode,List<Fragment>>>();
    String key;
    Map<ScanNode, List<Fragment>> m;
    List<Fragment> fragList;
    for (Entry<ScanNode, List<Fragment>> e : fragMap.entrySet()) {
      for (Fragment f : e.getValue()) {
        key = f.getPath().getName();
        if (hashed.containsKey(key)) {
          m = hashed.get(key);
        } else {
          m = new HashMap<>();
        }
        if (m.containsKey(e.getKey())) {
          fragList = m.get(e.getKey());
        } else {
          fragList = new ArrayList<>();
        }
        fragList.add(f);
        m.put(e.getKey(), fragList);
        hashed.put(key, m);
      }
    }
    
    return hashed;
  }
  
  private Collection<List<Fragment>> hashFragments(List<Fragment> frags) {
    SortedMap<String, List<Fragment>> hashed = new TreeMap<>();
    for (Fragment f : frags) {
      if (hashed.containsKey(f.getPath().getName())) {
        hashed.get(f.getPath().getName()).add(f);
      } else {
        List<Fragment> list = new ArrayList<>();
        list.add(f);
        hashed.put(f.getPath().getName(), list);
      }
    }

    return hashed.values();
  }

  public QueryUnit [] createLeafTasks(SubQuery subQuery) throws IOException {
    ScanNode[] scans = subQuery.getScanNodes();
    Preconditions.checkArgument(scans.length == 1, "Must be Scan Query");
    TableMeta meta;
    Path inputPath;

    ScanNode scan = scans[0];
    TableDesc desc = catalog.getTableDesc(scan.getTableId());
    inputPath = desc.getPath();
    meta = desc.getMeta();

    // TODO - should be change the inner directory
    Path oldPath = new Path(inputPath, "data");
    FileSystem fs = inputPath.getFileSystem(conf);
    if (fs.exists(oldPath)) {
      inputPath = oldPath;
    }
    List<Fragment> fragments = sm.getSplits(scan.getTableId(), meta, inputPath);

    QueryUnit queryUnit;
    List<QueryUnit> queryUnits = new ArrayList<>();

    int i = 0;
    for (Fragment fragment : fragments) {
      queryUnit = newQueryUnit(subQuery, i++);
      queryUnit.setFragment(scan.getTableId(), fragment);
      queryUnits.add(queryUnit);
    }

    return queryUnits.toArray(new QueryUnit[queryUnits.size()]);
  }
}
