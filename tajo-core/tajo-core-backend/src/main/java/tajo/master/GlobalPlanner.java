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

package tajo.master;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.event.EventHandler;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import tajo.QueryId;
import tajo.QueryIdFactory;
import tajo.QueryUnitAttemptId;
import tajo.SubQueryId;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.common.exception.NotImplementedException;
import tajo.conf.TajoConf;
import tajo.engine.parser.QueryBlock.FromTable;
import tajo.engine.planner.PlannerUtil;
import tajo.engine.planner.global.MasterPlan;
import tajo.engine.planner.logical.*;
import tajo.engine.utils.TupleUtil;
import tajo.master.ExecutionBlock.PartitionType;
import tajo.storage.Fragment;
import tajo.storage.StorageManager;
import tajo.storage.TupleRange;
import tajo.util.TajoIdUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
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
  
  /**
   * Transforms a logical plan to a two-phase plan. 
   * Store nodes are inserted for every logical nodes except store and scan nodes
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
  
  private Map<StoreTableNode, ExecutionBlock> convertMap =
      new HashMap<StoreTableNode, ExecutionBlock>();
  
  /**
   * Logical plan을 후위 탐색하면서 SubQuery 생성
   * 
   * @param node 현재 방문 중인 노드
   * @throws IOException
   */
  private void recursiveBuildSubQuery(LogicalNode node)
      throws IOException {
    ExecutionBlock subQuery;
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
        subQuery = new ExecutionBlock(id);

        switch (store.getSubNode().getType()) {
        case BST_INDEX_SCAN:
        case SCAN:  // store - scan
          subQuery = makeScanSubQuery(subQuery);
          subQuery.setPlan(node);
          break;
        case SELECTION:
        case PROJECTION:
        case LIMIT:
          subQuery = makeUnarySubQuery(store, node, subQuery);
          subQuery.setPlan(node);
          break;
        case GROUP_BY:
          subQuery = makeGroupbySubQuery(store, node, subQuery);
          subQuery.setPlan(node);
          break;
        case SORT:
          subQuery = makeSortSubQuery(store, node, subQuery);
          subQuery.setPlan(node);
          break;
        case JOIN:  // store - join
          subQuery = makeJoinSubQuery(store, node, subQuery);
          subQuery.setPlan(node);
          break;
        case UNION:
          subQuery = makeUnionSubQuery(store, node, subQuery);
          subQuery.setPlan(node);
          break;
        default:
          subQuery = null;
          break;
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
  
  private ExecutionBlock makeScanSubQuery(ExecutionBlock block) {
    block.setPartitionType(PartitionType.LIST);
    return block;
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
  private ExecutionBlock makeUnarySubQuery(StoreTableNode rootStore,
                                     LogicalNode plan, ExecutionBlock unit) throws IOException {
    ScanNode newScan;
    ExecutionBlock prev;
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
      prev.setParentBlock(unit);
      unit.addChildBlock(newScan, prev);
      prev.setPartitionType(PartitionType.LIST);
    }

    unit.setPartitionType(PartitionType.LIST);

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
  private ExecutionBlock makeGroupbySubQuery(StoreTableNode rootStore,
                                       LogicalNode plan, ExecutionBlock unit) throws IOException {
    UnaryNode unary = (UnaryNode) plan;
    UnaryNode unaryChild;
    StoreTableNode prevStore;
    ScanNode newScan;
    ExecutionBlock prev;
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
        prev.setParentBlock(unit);
        unit.addChildBlock(newScan, prev);
      }

      if (unaryChild.getSubNode().getType() == curType) {
        // the second phase
        unit.setPartitionType(PartitionType.LIST);
        if (prev != null) {
          prev.setPartitionType(PartitionType.HASH);
        }
      } else {
        // the first phase
        unit.setPartitionType(PartitionType.HASH);
        if (prev != null) {
          prev.setPartitionType(PartitionType.LIST);
        }
      }
    } else if (unaryChild.getSubNode().getType() == ExprType.SCAN) {
      // the first phase
      // store - groupby - scan
      unit.setPartitionType(PartitionType.HASH);
    } else if (unaryChild.getSubNode().getType() == ExprType.UNION) {
      _handleUnionNode(rootStore, (UnionNode)unaryChild.getSubNode(), unit, 
          null, PartitionType.LIST);
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
  private ExecutionBlock makeUnionSubQuery(StoreTableNode rootStore,
                                     LogicalNode plan, ExecutionBlock unit) throws IOException {
    UnaryNode unary = (UnaryNode) plan;
    StoreTableNode outerStore, innerStore;
    ExecutionBlock prev;
    UnionNode union = (UnionNode) unary.getSubNode();
    unit.setPartitionType(PartitionType.LIST);
    
    if (union.getOuterNode().getType() == ExprType.STORE) {
      outerStore = (StoreTableNode) union.getOuterNode();
      TableMeta outerMeta = TCatUtil.newTableMeta(outerStore.getOutSchema(),
          StoreType.CSV);
      insertOuterScan(union, outerStore.getTableName(), outerMeta);
      prev = convertMap.get(outerStore);
      if (prev != null) {
        prev.getStoreTableNode().setTableName(rootStore.getTableName());
        prev.setPartitionType(PartitionType.LIST);
        prev.setParentBlock(unit);
        unit.addChildBlock((ScanNode) union.getOuterNode(), prev);
      }
    } else if (union.getOuterNode().getType() == ExprType.UNION) {
      _handleUnionNode(rootStore, union, unit, null, PartitionType.LIST);
    }
    
    if (union.getInnerNode().getType() == ExprType.STORE) {
      innerStore = (StoreTableNode) union.getInnerNode();
      TableMeta innerMeta = TCatUtil.newTableMeta(innerStore.getOutSchema(),
          StoreType.CSV);
      insertInnerScan(union, innerStore.getTableName(), innerMeta);
      prev = convertMap.get(innerStore);
      if (prev != null) {
        prev.getStoreTableNode().setTableName(rootStore.getTableName());
        prev.setPartitionType(PartitionType.LIST);
        prev.setParentBlock(unit);
        unit.addChildBlock((ScanNode) union.getInnerNode(), prev);
      }
    } else if (union.getInnerNode().getType() == ExprType.UNION) {
      _handleUnionNode(rootStore, union, unit, null, PartitionType.LIST);
    }

    return unit;
  }

  private ExecutionBlock makeSortSubQuery(StoreTableNode rootStore,
                                    LogicalNode plan, ExecutionBlock unit) throws IOException {

    UnaryNode unary = (UnaryNode) plan;
    UnaryNode unaryChild;
    StoreTableNode prevStore;
    ScanNode newScan;
    ExecutionBlock prev;
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
        prev.setParentBlock(unit);
        unit.addChildBlock(newScan, prev);
        if (unaryChild.getSubNode().getType() == curType) {
          // TODO - this is duplicated code
          prev.setPartitionType(PartitionType.RANGE);
        } else {
          prev.setPartitionType(PartitionType.LIST);
        }
      }
      if (unaryChild.getSubNode().getType() == curType) {
        // the second phase
        unit.setPartitionType(PartitionType.LIST);
      } else {
        // the first phase
        unit.setPartitionType(PartitionType.HASH);
      }
    } else if (unaryChild.getSubNode().getType() == ExprType.SCAN) {
      // the first phase
      // store - sort - scan
      unit.setPartitionType(PartitionType.RANGE);
    } else if (unaryChild.getSubNode().getType() == ExprType.UNION) {
      _handleUnionNode(rootStore, (UnionNode)unaryChild.getSubNode(), unit,
          null, PartitionType.LIST);
    } else {
      // error
    }
    return unit;
  }
  
  private ExecutionBlock makeJoinSubQuery(StoreTableNode rootStore,
                                    LogicalNode plan, ExecutionBlock unit) throws IOException {
    UnaryNode unary = (UnaryNode)plan;
    StoreTableNode outerStore, innerStore;
    ExecutionBlock prev;
    JoinNode join = (JoinNode) unary.getSubNode();
    Schema outerSchema = join.getOuterNode().getOutSchema();
    Schema innerSchema = join.getInnerNode().getOutSchema();
    unit.setPartitionType(PartitionType.LIST);

    List<Column> outerCollist = new ArrayList<Column>();
    List<Column> innerCollist = new ArrayList<Column>();
    
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
        prev.setPartitionType(PartitionType.HASH);
        prev.setParentBlock(unit);
        unit.addChildBlock((ScanNode) join.getOuterNode(), prev);
      }
      outerStore.setPartitions(PartitionType.HASH, outerCols, 32);
    } else if (join.getOuterNode().getType() == ExprType.UNION) {
      _handleUnionNode(rootStore, (UnionNode)join.getOuterNode(), unit, 
          outerCols, PartitionType.HASH);
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
        prev.setPartitionType(PartitionType.HASH);
        prev.setParentBlock(unit);
        unit.addChildBlock((ScanNode) join.getInnerNode(), prev);
      }
      innerStore.setPartitions(PartitionType.HASH, innerCols, 32);
    } else if (join.getInnerNode().getType() == ExprType.UNION) {
      _handleUnionNode(rootStore, (UnionNode)join.getInnerNode(), unit,
          innerCols, PartitionType.HASH);
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
      ExecutionBlock cur, Column[] cols, PartitionType prevOutputType)
          throws IOException {
    StoreTableNode store;
    TableMeta meta;
    ExecutionBlock prev;
    
    if (union.getOuterNode().getType() == ExprType.STORE) {
      store = (StoreTableNode) union.getOuterNode();
      meta = TCatUtil.newTableMeta(store.getOutSchema(), StoreType.CSV);
      insertOuterScan(union, store.getTableName(), meta);
      prev = convertMap.get(store);
      if (prev != null) {
        prev.getStoreTableNode().setTableName(rootStore.getTableName());
        prev.setPartitionType(prevOutputType);
        prev.setParentBlock(cur);
        cur.addChildBlock((ScanNode) union.getOuterNode(), prev);
      }
      if (cols != null) {
        store.setPartitions(PartitionType.LIST, cols, 32);
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
        prev.setPartitionType(prevOutputType);
        prev.setParentBlock(cur);
        cur.addChildBlock((ScanNode) union.getInnerNode(), prev);
      }
      if (cols != null) {
        store.setPartitions(PartitionType.LIST, cols, 32);
      }
    } else if (union.getInnerNode().getType() == ExprType.UNION) {
      _handleUnionNode(rootStore, (UnionNode)union.getInnerNode(), cur, cols, 
          prevOutputType);
    }
  }

  @VisibleForTesting
  public ExecutionBlock createMultilevelGroupby(
      ExecutionBlock firstPhaseGroupby, Column[] keys)
      throws CloneNotSupportedException, IOException {
    ExecutionBlock secondPhaseGroupby = firstPhaseGroupby.getParentBlock();
    Preconditions.checkState(secondPhaseGroupby.getScanNodes().length == 1);

    ScanNode secondScan = secondPhaseGroupby.getScanNodes()[0];
    GroupbyNode secondGroupby = (GroupbyNode) secondPhaseGroupby.
        getStoreTableNode().getSubNode();
    ExecutionBlock newPhaseGroupby = new ExecutionBlock(
        QueryIdFactory.newSubQueryId(firstPhaseGroupby.getId().getQueryId()));
    LogicalNode tmp = PlannerUtil.findTopParentNode(
        firstPhaseGroupby.getPlan(), ExprType.GROUP_BY);
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
    newPhaseGroupby.setPlan(newStore);

    secondPhaseGroupby.removeChildBlock(secondScan);

    // update the scan node of last phase
    secondScan = GlobalPlannerUtils.newScanPlan(secondScan.getInSchema(),
        newPhaseGroupby.getOutputName(),
        sm.getTablePath(newPhaseGroupby.getOutputName()));
    secondScan.setLocal(true);
    secondGroupby.setSubNode(secondScan);
    secondPhaseGroupby.setPlan(secondPhaseGroupby.getPlan());

    // insert the new SubQuery
    // between the first phase and the second phase
    secondPhaseGroupby.addChildBlock(secondScan, newPhaseGroupby);
    newPhaseGroupby.addChildBlock(newPhaseGroupby.getScanNodes()[0],
        firstPhaseGroupby);
    newPhaseGroupby.setParentBlock(secondPhaseGroupby);
    firstPhaseGroupby.setParentBlock(newPhaseGroupby);

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
    ExecutionBlock root;
    
    if (index != null) {
      SubQueryId id = QueryIdFactory.newSubQueryId(queryId);
      ExecutionBlock unit = new ExecutionBlock(id);
      root = makeScanSubQuery(unit);
      root.setPlan(index);
    } else {
      root = convertMap.get(((LogicalRootNode)logicalPlan).getSubNode());
      root.getStoreTableNode().setLocal(false);
    }
    return new MasterPlan(root);
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
    ExecutionBlock execBlock = subQuery.getBlock();
    ScanNode[] scans = execBlock.getScanNodes();
    List<QueryUnit> queryUnits = new ArrayList<QueryUnit>();
    final int maxQueryUnitNum = n;
    
    if (execBlock.hasChildBlock()) {
      ExecutionBlock prev = execBlock.getChildBlock(scans[0]);
      switch (prev.getPartitionType()) {
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
      queryUnits = makeQueryUnitsForBinaryPlan(subQuery,
          queryUnits, fragMap);
    }

    return queryUnits;
  }

  public List<QueryUnit> makeQueryUnitsForBinaryPlan(
      SubQuery subQuery, List<QueryUnit> queryUnits,
      Map<ScanNode, List<Fragment>> fragmentMap) {
    ExecutionBlock execBlock = subQuery.getBlock();
    QueryUnit queryUnit;
    if (execBlock.hasJoin()) {
      // make query units for every composition of fragments of each scan
      Preconditions.checkArgument(fragmentMap.size()==2);

      ScanNode [] scanNodes = execBlock.getScanNodes();
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
    ExecutionBlock execBlock = subQuery.getBlock();
    QueryUnit unit = new QueryUnit(
        QueryIdFactory.newQueryUnitId(subQuery.getId()), execBlock.isLeafBlock(),
        subQuery.eventHandler);
    unit.setLogicalPlan(execBlock.getPlan());
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
    SortedMap<String, Map<ScanNode, List<URI>>> hashed =
        new TreeMap<String, Map<ScanNode, List<URI>>>();
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
          m = new HashMap<ScanNode, List<URI>>();
        }
        if (m.containsKey(e.getKey())) {
          uriList = m.get(e.getKey());
        } else {
          uriList = new ArrayList<URI>();
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
    SortedMap<String, List<URI>> hashed = new TreeMap<String, List<URI>>();
    String uriPath, key;
    for (URI uri : uriList) {
      // TODO
      uriPath = uri.toString();
      key = uriPath.substring(uriPath.lastIndexOf("=")+1);
      if (hashed.containsKey(key)) {
        hashed.get(key).add(uri);
      } else {
        List<URI> list = new ArrayList<URI>();
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
      map = new TreeMap<TupleRange, Set<URI>>();
    } else {
      map = new TreeMap<TupleRange, Set<URI>>(new TupleRange.DescendingTupleRangeComparator());
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
}
