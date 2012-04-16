package nta.engine.query;

/**
 * @author jihoon
 */

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import nta.catalog.CatalogService;
import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableDesc;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.engine.LogicalQueryUnitId;
import nta.engine.QueryId;
import nta.engine.QueryIdFactory;
import nta.engine.SubQueryId;
import nta.engine.cluster.QueryManager;
import nta.engine.cluster.QueryManager.WaitStatus;
import nta.engine.exec.eval.EvalTreeUtil;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.parser.QueryBlock.FromTable;
import nta.engine.planner.PlannerUtil;
import nta.engine.planner.global.MasterPlan;
import nta.engine.planner.global.QueryUnit;
import nta.engine.planner.global.ScheduleUnit;
import nta.engine.planner.global.ScheduleUnit.PARTITION_TYPE;
import nta.engine.planner.logical.BinaryNode;
import nta.engine.planner.logical.CreateIndexNode;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.GroupbyNode;
import nta.engine.planner.logical.JoinNode;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.LogicalNodeVisitor;
import nta.engine.planner.logical.LogicalRootNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.planner.logical.SortNode;
import nta.engine.planner.logical.StoreTableNode;
import nta.engine.planner.logical.UnaryNode;
import nta.engine.planner.logical.UnionNode;
import nta.storage.StorageManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class GlobalQueryPlanner {
  private static Log LOG = LogFactory.getLog(GlobalQueryPlanner.class);

  private StorageManager sm;
  private QueryManager qm;
  private CatalogService catalog;

  public GlobalQueryPlanner(StorageManager sm, QueryManager qm, CatalogService catalog)
      throws IOException {
    this.sm = sm;
    this.qm = qm;
    this.catalog = catalog;
  }

  public MasterPlan build(SubQueryId subQueryId, LogicalNode logicalPlan)
      throws IOException {
    // insert store at the subnode of the root
    UnaryNode root = (UnaryNode) logicalPlan;
    CreateIndexNode indexNode = null;
    // TODO: check whether the type of the subnode is CREATE_INDEX
    if (root.getSubNode().getType() == ExprType.CREATE_INDEX) {
      indexNode = (CreateIndexNode) root.getSubNode();
      root = (UnaryNode)root.getSubNode();
    } 
    if (root.getSubNode().getType() != ExprType.STORE) {
      insertStore(QueryIdFactory.newLogicalQueryUnitId().toString(), root).setLocal(false);
    }
    
    // convert 2-phase plan
    LogicalNode tp = convertTo2Phase(logicalPlan);

    // make query graph
    MasterPlan globalPlan = convertToGlobalPlan(subQueryId, indexNode, tp);

    return globalPlan;
  }
  
  private StoreTableNode insertStore(String tableId, LogicalNode parent) {
    StoreTableNode store = new StoreTableNode(tableId);
    store.setLocal(true);
    PlannerUtil.insertNode(parent, store);
    return store;
  }
  
  private class GlobalPlanBuilder implements LogicalNodeVisitor {
    @Override
    public void visit(LogicalNode node) {
      String tableId;
      if (node.getType() == ExprType.GROUP_BY) {
        GroupbyNode groupby = (GroupbyNode) node;
        if (groupby.getSubNode().getType() != ExprType.UNION &&
            groupby.getSubNode().getType() != ExprType.STORE &&
            groupby.getSubNode().getType() != ExprType.SCAN) {
          tableId = QueryIdFactory.newLogicalQueryUnitId().toString();
          insertStore(tableId, groupby);
        }
        tableId = QueryIdFactory.newLogicalQueryUnitId().toString();
        PlannerUtil.transformGroupbyTo2PWithStore((GroupbyNode)node, tableId);
      } else if (node.getType() == ExprType.SORT) {
        SortNode sort = (SortNode) node;
        if (sort.getSubNode().getType() != ExprType.UNION &&
            sort.getSubNode().getType() != ExprType.STORE &&
            sort.getSubNode().getType() != ExprType.SCAN) {
          tableId = QueryIdFactory.newLogicalQueryUnitId().toString();
          insertStore(tableId, sort);
        }
        tableId = QueryIdFactory.newLogicalQueryUnitId().toString();
        PlannerUtil.transformSortTo2PWithStore((SortNode)node, tableId);
      } else if (node.getType() == ExprType.JOIN) {
        JoinNode join = (JoinNode) node;
        if (join.getOuterNode().getType() != ExprType.UNION &&
            join.getOuterNode().getType() != ExprType.STORE) {
          tableId = QueryIdFactory.newLogicalQueryUnitId().toString();
          StoreTableNode store = new StoreTableNode(tableId);
          store.setLocal(true);
          PlannerUtil.insertOuterNode(node, store);
        }
        if (join.getInnerNode().getType() != ExprType.UNION &&
            join.getInnerNode().getType() != ExprType.STORE) {
          tableId = QueryIdFactory.newLogicalQueryUnitId().toString();
          StoreTableNode store = new StoreTableNode(tableId);
          store.setLocal(true);
          PlannerUtil.insertInnerNode(node, store);
        }
      } else if (node.getType() == ExprType.UNION) {
        UnionNode union = (UnionNode) node;
        if (union.getOuterNode().getType() != ExprType.UNION &&
            union.getOuterNode().getType() != ExprType.STORE) {
          tableId = QueryIdFactory.newLogicalQueryUnitId().toString();
          StoreTableNode store = new StoreTableNode(tableId);
          store.setLocal(true);
          PlannerUtil.insertOuterNode(node, store);
        }
        if (union.getInnerNode().getType() != ExprType.UNION &&
            union.getInnerNode().getType() != ExprType.STORE) {
          tableId = QueryIdFactory.newLogicalQueryUnitId().toString();
          StoreTableNode store = new StoreTableNode(tableId);
          store.setLocal(true);
          PlannerUtil.insertInnerNode(node, store);
        }
      }
    }
  }

  private LogicalNode convertTo2Phase(LogicalNode logicalPlan) {
    LogicalRootNode root = (LogicalRootNode) logicalPlan;
    root.postOrder(new GlobalPlanBuilder());
    return logicalPlan;
  }
  
  private Map<StoreTableNode, ScheduleUnit> convertMap = 
      new HashMap<StoreTableNode, ScheduleUnit>();
  
  private void recursiveBuildScheduleUnit(LogicalNode node) 
      throws IOException {
    ScheduleUnit unit = null;
    StoreTableNode store;
    if (node instanceof UnaryNode) {
      recursiveBuildScheduleUnit(((UnaryNode) node).getSubNode());
      
      if (node.getType() == ExprType.STORE) {
        store = (StoreTableNode) node;
        LogicalQueryUnitId id = null;
        if (store.getTableName().startsWith(QueryId.PREFIX)) {
          id = new LogicalQueryUnitId(store.getTableName());
        } else {
          id = QueryIdFactory.newLogicalQueryUnitId();
        }
        unit = new ScheduleUnit(id);

        switch (store.getSubNode().getType()) {
        case SCAN:  // store - scan
          unit = makeScanUnit(unit);
          unit.setLogicalPlan(node);
          break;
        case SELECTION:
        case PROJECTION:
          unit = makeUnifiableUnit(store, store.getSubNode(), unit);
          unit.setLogicalPlan(node);
          break;
        case GROUP_BY:
        case SORT:
          unit = makeTwoPhaseUnit(store, node, unit);
          unit.setLogicalPlan(node);
          break;
        case JOIN:  // store - join
          unit = makeJoinUnit(store, node, unit);
          unit.setLogicalPlan(node);
          break;
        case UNION:
          unit = makeUnionUnit(store, node, unit);
          unit.setLogicalPlan(node);
          break;
        default:
          unit = null;
          break;
        }
        convertMap.put(store, unit);
      }
    } else if (node instanceof BinaryNode) {
      recursiveBuildScheduleUnit(((BinaryNode) node).getOuterNode());
      recursiveBuildScheduleUnit(((BinaryNode) node).getInnerNode());
    } else {
      
    }
  }
  
  private ScheduleUnit makeScanUnit(ScheduleUnit unit) {
    unit.setOutputType(PARTITION_TYPE.LIST);
    return unit;
  }
  
  private ScheduleUnit makeUnifiableUnit(StoreTableNode rootStore, 
      LogicalNode plan, ScheduleUnit unit) throws IOException {
    UnaryNode unary = (UnaryNode) plan;
    switch (unary.getSubNode().getType()) {
    case SCAN:
      unit = makeScanUnit(unit);
      break;
    case SELECTION:
    case PROJECTION:
      unit = makeUnifiableUnit(rootStore, unary.getSubNode(), unit);
      break;
    case SORT:
    case GROUP_BY:
      unit = makeTwoPhaseUnit(rootStore, plan, unit);
      break;
    case JOIN:
      unit = makeJoinUnit(rootStore, plan, unit);
      break;
    case UNION:
      unit = makeUnionUnit(rootStore, plan, unit);
      break;
    }
    return unit;
  }
  
  private ScheduleUnit makeTwoPhaseUnit(StoreTableNode rootStore, 
      LogicalNode plan, ScheduleUnit unit) throws IOException {
    UnaryNode unary = (UnaryNode) plan;
    UnaryNode unaryChild;
    StoreTableNode prevStore;
    ScanNode newScan;
    ScheduleUnit prev;
    unaryChild = (UnaryNode) unary.getSubNode();  // groupby
    ExprType curType = unaryChild.getType();
    if (unaryChild.getSubNode().getType() == ExprType.STORE) {
      // store - groupby - store
      unaryChild = (UnaryNode) unaryChild.getSubNode(); // store
      prevStore = (StoreTableNode) unaryChild;
      TableMeta meta = TCatUtil.newTableMeta(prevStore.getOutputSchema(), 
          StoreType.CSV);
      newScan = (ScanNode)insertScan(unary.getSubNode(), 
          prevStore.getTableName(), meta);
      prev = convertMap.get(prevStore);
      if (prev != null) {
        prev.setNextQuery(unit);
        unit.addPrevQuery(newScan, prev);
        if (unaryChild.getSubNode().getType() == curType) {
          prev.setOutputType(PARTITION_TYPE.HASH);
        } else {
          prev.setOutputType(PARTITION_TYPE.LIST);
        }
      }
      if (unaryChild.getSubNode().getType() == curType) {
        unit.setOutputType(PARTITION_TYPE.LIST);
      } else {
        unit.setOutputType(PARTITION_TYPE.HASH);
      }
    } else if (unaryChild.getSubNode().getType() == ExprType.SCAN) {
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
  
  private ScheduleUnit makeUnionUnit(StoreTableNode rootStore, 
      LogicalNode plan, ScheduleUnit unit) throws IOException {
    UnaryNode unary = (UnaryNode) plan;
    StoreTableNode outerStore, innerStore;
    ScheduleUnit prev;
    UnionNode union = (UnionNode) unary.getSubNode();
    unit.setOutputType(PARTITION_TYPE.LIST);
    
    if (union.getOuterNode().getType() == ExprType.STORE) {
      outerStore = (StoreTableNode) union.getOuterNode();
      TableMeta outerMeta = TCatUtil.newTableMeta(outerStore.getOutputSchema(), 
          StoreType.CSV);
      insertOuterScan(union, outerStore.getTableName(), outerMeta);
      prev = convertMap.get(outerStore);
      if (prev != null) {
        prev.getStoreTableNode().setTableName(rootStore.getTableName());
        prev.setOutputType(PARTITION_TYPE.LIST);
        prev.setNextQuery(unit);
        unit.addPrevQuery((ScanNode)union.getOuterNode(), prev);
      }
    } else if (union.getOuterNode().getType() == ExprType.UNION) {
      _handleUnionNode(rootStore, union, unit, null, PARTITION_TYPE.LIST);
    }
    
    if (union.getInnerNode().getType() == ExprType.STORE) {
      innerStore = (StoreTableNode) union.getInnerNode();
      TableMeta innerMeta = TCatUtil.newTableMeta(innerStore.getOutputSchema(), 
          StoreType.CSV);
      insertInnerScan(union, innerStore.getTableName(), innerMeta);
      prev = convertMap.get(innerStore);
      if (prev != null) {
        prev.getStoreTableNode().setTableName(rootStore.getTableName());
        prev.setOutputType(PARTITION_TYPE.LIST);
        prev.setNextQuery(unit);
        unit.addPrevQuery((ScanNode)union.getInnerNode(), prev);
      }
    } else if (union.getInnerNode().getType() == ExprType.UNION) {
      _handleUnionNode(rootStore, union, unit, null, PARTITION_TYPE.LIST);
    }

    return unit;
  }
  
  private ScheduleUnit makeJoinUnit(StoreTableNode rootStore, 
      LogicalNode plan, ScheduleUnit unit) throws IOException {
    UnaryNode unary = (UnaryNode)plan;
    StoreTableNode outerStore, innerStore;
    ScheduleUnit prev;
    JoinNode join = (JoinNode) unary.getSubNode();
    Schema outerSchema = join.getOuterNode().getOutputSchema();
    Schema innerSchema = join.getInnerNode().getOutputSchema();
    unit.setOutputType(PARTITION_TYPE.LIST);
    List<Column> outerCollist = new ArrayList<Column>();
    List<Column> innerCollist = new ArrayList<Column>();   
    
    // TODO: set partition for store nodes
    if (join.hasJoinQual()) {
      // repartition
      Set<Column> cols = EvalTreeUtil.findDistinctRefColumns(join.getJoinQual());
      Iterator<Column> it = cols.iterator();
      while (it.hasNext()) {
        Column col = it.next();
        if (outerSchema.contains(col.getQualifiedName())) {
          outerCollist.add(col);
        } else if (innerSchema.contains(col.getQualifiedName())) {
          innerCollist.add(col);
        }
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
      TableMeta outerMeta = TCatUtil.newTableMeta(outerStore.getOutputSchema(), 
          StoreType.CSV);
      insertOuterScan(join, outerStore.getTableName(), outerMeta);
      prev = convertMap.get(outerStore);
      if (prev != null) {
        prev.setOutputType(PARTITION_TYPE.HASH);
        prev.setNextQuery(unit);
        unit.addPrevQuery((ScanNode)join.getOuterNode(), prev);
      }
      outerStore.setPartitions(outerCols, 1);
    } else if (join.getOuterNode().getType() != ExprType.UNION) {
      _handleUnionNode(rootStore, (UnionNode)join.getOuterNode(), unit, 
          outerCols, PARTITION_TYPE.HASH);
    }
    
    // inner
    if (join.getInnerNode().getType() == ExprType.STORE) {
      innerStore = (StoreTableNode) join.getInnerNode();
      TableMeta innerMeta = TCatUtil.newTableMeta(innerStore.getOutputSchema(), 
          StoreType.CSV);
      insertInnerScan(join, innerStore.getTableName(), innerMeta);
      prev = convertMap.get(innerStore);
      if (prev != null) {
        prev.setOutputType(PARTITION_TYPE.HASH);
        prev.setNextQuery(unit);
        unit.addPrevQuery((ScanNode)join.getInnerNode(), prev);
      }
      innerStore.setPartitions(innerCols, 1);
    } else if (join.getInnerNode().getType() == ExprType.UNION) {
      _handleUnionNode(rootStore, (UnionNode)join.getInnerNode(), unit,
          innerCols, PARTITION_TYPE.HASH);
    }
    
    return unit;
  }
  
  private void _handleUnionNode(StoreTableNode rootStore, UnionNode union, 
      ScheduleUnit cur, Column[] cols, PARTITION_TYPE prevOutputType) 
          throws IOException {
    StoreTableNode store;
    TableMeta meta;
    ScheduleUnit prev;
    
    if (union.getOuterNode().getType() == ExprType.STORE) {
      store = (StoreTableNode) union.getOuterNode();
      meta = TCatUtil.newTableMeta(store.getOutputSchema(), StoreType.CSV);
      insertOuterScan(union, store.getTableName(), meta);
      prev = convertMap.get(store);
      if (prev != null) {
        prev.getStoreTableNode().setTableName(rootStore.getTableName());
        prev.setOutputType(prevOutputType);
        prev.setNextQuery(cur);
        cur.addPrevQuery((ScanNode)union.getOuterNode(), prev);
      }
      if (cols != null) {
        store.setPartitions(cols, 1);
      }
    } else if (union.getOuterNode().getType() == ExprType.UNION) {
      _handleUnionNode(rootStore, (UnionNode)union.getOuterNode(), cur, cols, 
          prevOutputType);
    }
    
    if (union.getInnerNode().getType() == ExprType.STORE) {
      store = (StoreTableNode) union.getInnerNode();
      meta = TCatUtil.newTableMeta(store.getOutputSchema(), StoreType.CSV);
      insertInnerScan(union, store.getTableName(), meta);
      prev = convertMap.get(store);
      if (prev != null) {
        prev.getStoreTableNode().setTableName(rootStore.getTableName());
        prev.setOutputType(prevOutputType);
        prev.setNextQuery(cur);
        cur.addPrevQuery((ScanNode)union.getInnerNode(), prev);
      }
      if (cols != null) {
        store.setPartitions(cols, 1);
      }
    } else if (union.getInnerNode().getType() == ExprType.UNION) {
      _handleUnionNode(rootStore, (UnionNode)union.getInnerNode(), cur, cols, 
          prevOutputType);
    }
  }
  
  private LogicalNode insertScan(LogicalNode parent, String tableId, TableMeta meta) 
      throws IOException {
    TableDesc desc = TCatUtil.newTableDesc(tableId, meta, sm.getTablePath(tableId));
    ScanNode newScan = new ScanNode(new FromTable(desc));
    newScan.setLocal(true);
    newScan.setInputSchema(meta.getSchema());
    newScan.setOutputSchema(meta.getSchema());
    ((UnaryNode)parent).setSubNode(newScan);
    return newScan;
  }
  
  private LogicalNode insertOuterScan(BinaryNode parent, String tableId, 
      TableMeta meta) throws IOException {
    TableDesc desc = TCatUtil.newTableDesc(tableId, meta, sm.getTablePath(tableId));
    ScanNode scan = new ScanNode(new FromTable(desc));
    scan.setLocal(true);
    scan.setInputSchema(meta.getSchema());
    scan.setOutputSchema(meta.getSchema());
    parent.setOuter(scan);
    return parent;
  }
  
  private LogicalNode insertInnerScan(BinaryNode parent, String tableId, 
      TableMeta meta) throws IOException {
    TableDesc desc = TCatUtil.newTableDesc(tableId, meta, sm.getTablePath(tableId));
    ScanNode scan = new ScanNode(new FromTable(desc));
    scan.setLocal(true);
    scan.setInputSchema(meta.getSchema());
    scan.setOutputSchema(meta.getSchema());
    parent.setInner(scan);
    return parent;
  }
  
  private MasterPlan convertToGlobalPlan(SubQueryId subQueryId,
      CreateIndexNode index, LogicalNode logicalPlan) throws IOException {
    recursiveBuildScheduleUnit(logicalPlan);
    ScheduleUnit root = convertMap.get(((LogicalRootNode)logicalPlan).getSubNode());
    if (index != null) {
      index.setSubNode(root.getLogicalPlan());
      root.setLogicalPlan(index);
    }
    return new MasterPlan(root);
  }

  public QueryUnit[] localize(ScheduleUnit unit, int n) 
      throws IOException, URISyntaxException {
    FileStatus[] files;
    Fragment[] frags;
    List<Fragment> fragList;
    List<URI> uriList;
    Map<ScanNode, List<Fragment>> fragMap = new HashMap<ScanNode, List<Fragment>>();
    Map<ScanNode, List<URI>> fetchMap = new HashMap<ScanNode, List<URI>>();
    
    // if the next query is join, 
    // set the partition number for the current logicalUnit
    // TODO: union handling when join has union as its child
    ScheduleUnit nextQueryUnit = unit.getNextQuery();
    if (nextQueryUnit != null) {
      if (nextQueryUnit.getStoreTableNode().getSubNode().getType() == ExprType.JOIN) {
        unit.getStoreTableNode().setPartitions(
            unit.getStoreTableNode().getPartitionKeys(), n);
      }
    }

    // set the partition number for groupby and sort
    if (unit.getOutputType() == PARTITION_TYPE.HASH) {
      StoreTableNode store = unit.getStoreTableNode();
      Column[] keys = null;
      if (store.getSubNode().getType() == ExprType.GROUP_BY) {
        GroupbyNode groupby = (GroupbyNode)store.getSubNode();
        keys = groupby.getGroupingColumns();
      } else if (store.getSubNode().getType() == ExprType.SORT) {
        SortNode sort = (SortNode)store.getSubNode();
        keys = new Column[sort.getSortKeys().length];
        for (int i = 0; i < keys.length; i++) {
          keys[i] = sort.getSortKeys()[i].getSortKey();
        }
      }
      if (keys != null) {
        if (keys.length == 0) {
//          store.clearPartitions();
          store.setPartitions(new Column[]{}, 1);
        } else {
          store.setPartitions(keys, n);
        }
      }
    }

    Path tablepath = null;
    ScanNode[] scans = unit.getScanNodes();
    for (ScanNode scan : scans) {
      if (scan.getTableId().startsWith(QueryId.PREFIX)) {
        tablepath = sm.getTablePath(scan.getTableId());
      } else {
        tablepath = catalog.getTableDesc(scan.getTableId()).getPath();
      }
      if (scan.isLocal()) {
        // make fetchMap
        uriList = new ArrayList<URI>();
        fragList = new ArrayList<Fragment>();
        for (WaitStatus status: qm.getWaitStatusOfLogicalUnit(
            unit.getPrevQuery(scan))) {
          int cnt = status.getInProgressStatus().getPartitionsCount();
          for (int i = 0; i < cnt; i++) {
            uriList.add(new URI(status.getInProgressStatus().
                getPartitions(i).getFileName()));
          }
        }
        fetchMap.put(scan, uriList);
        Fragment frag = new Fragment(scan.getTableId(), tablepath,
            TCatUtil.newTableMeta(scan.getInputSchema(),StoreType.CSV), 
            0, 0);
        fragList.add(frag);
        fragMap.put(scan, fragList);
      } else {
        fragList = new ArrayList<Fragment>();
        // set fragments for each scan
        if (unit.hasPrevQuery() && 
            unit.getPrevQuery(scan).getOutputType() == 
            PARTITION_TYPE.HASH) {
          files = sm.getFileSystem().listStatus(tablepath);
        } else {
          files = new FileStatus[1];
          files[0] = sm.getFileSystem().getFileStatus(tablepath);
        }
        for (FileStatus file : files) {
          frags = sm.split(file.getPath());
          for (Fragment f : frags) {
            // TODO: the fragment ID should be set before
            f.setId(scan.getTableId());
            fragList.add(f);
          }
        }
        fragMap.put(scan, fragList);
      }
    }

    QueryUnit[] units = split(unit, n);
    for (ScanNode scan : scans) {
      if (unit.hasPrevQuery()) {
        ScheduleUnit prev = unit.getPrevQuery(scan);
        if (prev.getStoreTableNode().getSubNode().getType() == 
            ExprType.GROUP_BY) {
          GroupbyNode groupby = (GroupbyNode) prev.getStoreTableNode().
              getSubNode();
          if (groupby.getGroupingColumns().length == 0) {
            units = split(unit, 1);
            
          }
        }
        switch (prev.getOutputType()) {
        case BROADCAST:
          assignFetchesByBroadcast(units, fetchMap.get(scan));
          break;
        case HASH:
          if (scan.isLocal()) {
            assignFetchesByHash(units, fetchMap.get(scan));
            assignEqualFragment(units, fragMap.get(scan).get(0));
          } else {
            assignFragmentsByHash(units, fragMap.get(scan));
          }
          break;
        case LIST:
          if (scan.isLocal()) {
            assignFetchesByRoundRobin(units, scan, fetchMap.get(scan));
            assignEqualFragment(units, fragMap.get(scan).get(0));
          } else {
            assignFragmentsByRoundRobin(units, fragMap.get(scan));
          }
          break;
        }
      } else {
        assignFragmentsByRoundRobin(units, fragMap.get(scan));
      }
    }
    
    int cnt = 0;
    for (QueryUnit u : units) {
      if (!(u.getFetches().isEmpty() && 
          u.getFragments().isEmpty())) {
        cnt++;
      }
    }
    if (cnt != units.length) {
      QueryUnit[] uus = new QueryUnit[cnt];
      int i = 0;
      for (int j = 0; j < units.length; j++) {
        if (!(units[j].getFetches().isEmpty() &&
            units[j].getFragments().isEmpty())) {
          uus[i++] = units[j];
        }
      }
      units = uus;
    }
    
    unit.setQueryUnits(units);
    
    return units;
  }
  
  private QueryUnit[] split(ScheduleUnit logicalUnit, int n) {
    QueryUnit[] units = new QueryUnit[n];
    for (int i = 0; i < units.length; i++) {
      units[i] = new QueryUnit(QueryIdFactory.newQueryUnitId());
      units[i].setLogicalPlan(logicalUnit.getLogicalPlan());
    }
    return units;
  }
  
  private void assignFetchesByHash(QueryUnit[] units, List<URI> uriList) {
    Map<String, List<URI>> hashed = hashFetches(uriList); // hash key, uri
    int i = 0;
    // TODO: units에 hashed 할당
    Iterator<Entry<String, List<URI>>> it = hashed.entrySet().iterator();
    Entry<String, List<URI>> e;
    while (it.hasNext()) {
      e = it.next();
      units[i].addFetches(e.getKey(), e.getValue());
      if (++i == units.length) {
        i = 0;
      }
    }
  }
  
  private void assignFetchesByRoundRobin(QueryUnit[] units, ScanNode scan,
      List<URI> uriList) { 
    int i = 0;
    for (URI uri : uriList) {
      units[i].addFetch(scan.getTableId(), uri);
      if (++i == units.length) {
        i = 0;
      }
    }
  }
  
  private Map<String, List<URI>> hashFetches(List<URI> uriList) {
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
    
    return hashed;
  }
  
  private void assignFetchesByBroadcast(QueryUnit[] units, List<URI> uriList) {
    for (URI uri : uriList) {
      for (QueryUnit unit : units) {
        // TODO: add each uri to every units
        unit.addFetch("b", uri);
      }
    }
  }
  
  private void assignFragmentsByHash(QueryUnit[] units, List<Fragment> fragList) {
    Collection<List<Fragment>> hashed = hashFragments(fragList);
    int i = 0;
    for (List<Fragment> frags : hashed) {
      units[i++].addFragments(frags.toArray(new Fragment[frags.size()]));
      if (i == units.length) {
        i = 0;
      }
    }
  }

  private void assignFragmentsByRoundRobin(QueryUnit[] units,
      List<Fragment> frags) {
    int i = 0;
    for (Fragment f : frags) {
      units[i].addFragment(f);
      if (++i == units.length) {
        i = 0;
      }
    }
  }
  
  private void assignEqualFragment(QueryUnit[] units, Fragment frag) {
    for (QueryUnit unit : units) {
      unit.addFragment(frag);
    }
  }
  
  private Collection<List<Fragment>> hashFragments(List<Fragment> frags) {
    SortedMap<String, List<Fragment>> hashed = new TreeMap<String, List<Fragment>>();
    for (Fragment f : frags) {
      if (hashed.containsKey(f.getPath().getName())) {
        hashed.get(f.getPath().getName()).add(f);
      } else {
        List<Fragment> list = new ArrayList<Fragment>();
        list.add(f);
        hashed.put(f.getPath().getName(), list);
      }
    }

    return hashed.values();
  }
}
