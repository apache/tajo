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

import nta.catalog.Column;
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
import nta.engine.planner.global.LogicalQueryUnit;
import nta.engine.planner.global.LogicalQueryUnit.PARTITION_TYPE;
import nta.engine.planner.global.LogicalQueryUnitGraph;
import nta.engine.planner.global.QueryUnit;
import nta.engine.planner.logical.BinaryNode;
import nta.engine.planner.logical.CreateIndexNode;
import nta.engine.planner.logical.StoreTableNode;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.GroupbyNode;
import nta.engine.planner.logical.JoinNode;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.LogicalNodeVisitor;
import nta.engine.planner.logical.LogicalRootNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.planner.logical.SortNode;
import nta.engine.planner.logical.UnaryNode;
import nta.storage.StorageManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;

public class GlobalQueryPlanner {
  private static Log LOG = LogFactory.getLog(GlobalQueryPlanner.class);

  private StorageManager sm;
  private QueryManager qm;

  public GlobalQueryPlanner(StorageManager sm, QueryManager qm)
      throws IOException {
    this.sm = sm;
    this.qm = qm;
  }

  public LogicalQueryUnitGraph build(SubQueryId subQueryId, LogicalNode logicalPlan)
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
    LogicalQueryUnitGraph globalPlan = convertToGlobalPlan(subQueryId, indexNode, tp);

    return globalPlan;
  }
  
  private StoreTableNode insertStore(String tableId, LogicalNode parent) {
    StoreTableNode store = new StoreTableNode(tableId);
    store.setLocal(true);
    PlannerUtil.insertNode(parent, store);
    return store;
  }
  
  private class TwoPhaseBuilder implements LogicalNodeVisitor {
    @Override
    public void visit(LogicalNode node) {
      String tableId;
      if (node.getType() == ExprType.GROUP_BY) {
        GroupbyNode groupby = (GroupbyNode) node;
        if (groupby.getSubNode().getType() != ExprType.STORE &&
            groupby.getSubNode().getType() != ExprType.SCAN) {
          tableId = QueryIdFactory.newLogicalQueryUnitId().toString();
          insertStore(tableId, groupby);
        }
        tableId = QueryIdFactory.newLogicalQueryUnitId().toString();
        PlannerUtil.transformGroupbyTo2PWithStore((GroupbyNode)node, tableId);
      } else if (node.getType() == ExprType.SORT) {
        SortNode sort = (SortNode) node;
        if (sort.getSubNode().getType() != ExprType.STORE &&
            sort.getSubNode().getType() != ExprType.SCAN) {
          tableId = QueryIdFactory.newLogicalQueryUnitId().toString();
          insertStore(tableId, sort);
        }
        tableId = QueryIdFactory.newLogicalQueryUnitId().toString();
        PlannerUtil.transformSortTo2PWithStore((SortNode)node, tableId);
      } else if (node.getType() == ExprType.JOIN) {
        JoinNode join = (JoinNode) node;
        if (join.getOuterNode().getType() != ExprType.STORE) {
          tableId = QueryIdFactory.newLogicalQueryUnitId().toString();
          StoreTableNode store = new StoreTableNode(tableId);
          store.setLocal(true);
          PlannerUtil.insertOuterNode(node, store);
        }
        if (join.getInnerNode().getType() != ExprType.STORE) {
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
    root.postOrder(new TwoPhaseBuilder());
    return logicalPlan;
  }
  
  private Map<StoreTableNode, LogicalQueryUnit> convertMap = 
      new HashMap<StoreTableNode, LogicalQueryUnit>();
  
  private void recursiveBuildQueryUnit(LogicalNode node) 
      throws IOException {
    LogicalQueryUnit unit = null, prev = null;
    UnaryNode unaryChild;
    StoreTableNode store, prevStore;
    ScanNode newScan;
    if (node instanceof UnaryNode) {
      recursiveBuildQueryUnit(((UnaryNode) node).getSubNode());
      
      if (node.getType() == ExprType.STORE) {
        store = (StoreTableNode) node;
        LogicalQueryUnitId id = null;
        if (store.getTableName().startsWith(QueryId.PREFIX)) {
          id = new LogicalQueryUnitId(store.getTableName());
        } else {
          id = QueryIdFactory.newLogicalQueryUnitId();
        }
        unit = new LogicalQueryUnit(id);

        switch (store.getSubNode().getType()) {
        case SCAN:  // store - scan
          unit.setOutputType(PARTITION_TYPE.LIST);
          unit.setLogicalPlan(node);
          break;
        case GROUP_BY:
        case SORT:
          unaryChild = (UnaryNode) store.getSubNode();  // groupby
          if (unaryChild.getSubNode().getType() == ExprType.STORE) {
            // store - groupby - store
            unaryChild = (UnaryNode) unaryChild.getSubNode(); // store
            if (unaryChild.getSubNode().getType() == ExprType.SCAN) {
              // store - groupby - store - scan
              // TODO: impossible case
            } else {
              // store - groupby - store - groupby
              unit.setOutputType(PARTITION_TYPE.LIST);
              prevStore = (StoreTableNode) unaryChild;
              TableMeta meta = TCatUtil.newTableMeta(prevStore.getOutputSchema(), 
                  StoreType.CSV);
              newScan = (ScanNode)insertScan(store.getSubNode(), 
                  prevStore.getTableName(), meta);
              prev = convertMap.get(prevStore);
              if (prev != null) {
                prev.setOutputType(PARTITION_TYPE.HASH);
                prev.setNextQuery(unit);
                unit.addPrevQuery(newScan, prev);
              }
            }
          } else if (unaryChild.getSubNode().getType() == ExprType.SCAN) {
            // store - groupby - scan
            unit.setOutputType(PARTITION_TYPE.HASH);
          }
          
          unit.setLogicalPlan(node);
          break;
        case JOIN:  // store - join
          JoinNode join = (JoinNode) store.getSubNode();
          unit.setOutputType(PARTITION_TYPE.LIST);
          
          prevStore = (StoreTableNode) join.getOuterNode();
          StoreTableNode prevStore2 = (StoreTableNode) join.getInnerNode();
          TableMeta lmeta = TCatUtil.newTableMeta(prevStore.getOutputSchema(), 
              StoreType.CSV);
          TableMeta rmeta = TCatUtil.newTableMeta(prevStore2.getOutputSchema(), 
              StoreType.CSV);
          insertScan(join, prevStore.getTableName(), prevStore2.getTableName(), 
              lmeta, rmeta);
          
          prev = convertMap.get(prevStore);
          if (prev != null) {
            prev.setOutputType(PARTITION_TYPE.HASH);
            prev.setNextQuery(unit);
            unit.addPrevQuery((ScanNode)join.getOuterNode(), prev);
          }
          
          prev = convertMap.get(prevStore2);
          if (prev != null) {
            prev.setOutputType(PARTITION_TYPE.HASH);
            prev.setNextQuery(unit);
            unit.addPrevQuery((ScanNode)join.getInnerNode(), prev);
          }
          
          // TODO: set partition for store nodes
          if (join.hasJoinQual()) {
            // repartition
            Set<Column> cols = EvalTreeUtil.findDistinctRefColumns(join.getJoinQual());
            Iterator<Column> it = cols.iterator();
            List<Column> leftCols = new ArrayList<Column>();
            List<Column> rightCols = new ArrayList<Column>();
            while (it.hasNext()) {
              Column col = it.next();
              if (prevStore.getOutputSchema().contains(col.getQualifiedName())) {
                leftCols.add(col);
              } else if (prevStore2.getOutputSchema().contains(col.getQualifiedName())) {
                rightCols.add(col);
              }
            }
            prevStore.setPartitions(leftCols.toArray(
                new Column[leftCols.size()]), 1);
            prevStore2.setPartitions(rightCols.toArray(
                new Column[rightCols.size()]), 1);
          } else {
            // broadcast
          }
          
          unit.setLogicalPlan(node);
          break;
        default:
          unit = null;
          break;
        }
        convertMap.put(store, unit);
      }
    } else if (node instanceof BinaryNode) {
      recursiveBuildQueryUnit(((BinaryNode) node).getOuterNode());
      recursiveBuildQueryUnit(((BinaryNode) node).getInnerNode());
    } else {
      
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
  
  private LogicalNode insertScan(LogicalNode parent, String leftId, String rightId, 
      TableMeta lmeta, TableMeta rmeta) throws IOException {
    TableDesc desc = TCatUtil.newTableDesc(leftId, lmeta, sm.getTablePath(leftId));
    ScanNode left = new ScanNode(new FromTable(desc));
    left.setLocal(true);
    left.setInputSchema(lmeta.getSchema());
    left.setOutputSchema(lmeta.getSchema());
    desc = TCatUtil.newTableDesc(rightId, rmeta, sm.getTablePath(rightId));
    ScanNode right = new ScanNode(new FromTable(desc));
    right.setLocal(true);
    right.setInputSchema(rmeta.getSchema());
    right.setOutputSchema(rmeta.getSchema());
    ((BinaryNode)parent).setInner(right);
    ((BinaryNode)parent).setOuter(left);
    return parent;
  }
  
  private LogicalQueryUnitGraph convertToGlobalPlan(SubQueryId subQueryId,
      CreateIndexNode index, LogicalNode logicalPlan) throws IOException {
    recursiveBuildQueryUnit(logicalPlan);
    LogicalQueryUnit root = convertMap.get(((LogicalRootNode)logicalPlan).getSubNode());
    if (index != null) {
      index.setSubNode(root.getLogicalPlan());
      root.setLogicalPlan(index);
    }
    return new LogicalQueryUnitGraph(root);
  }

  public QueryUnit[] localize(LogicalQueryUnit logicalUnit, int n) 
      throws IOException, URISyntaxException {
    FileStatus[] files;
    Fragment[] frags;
    List<Fragment> fragList = new ArrayList<Fragment>();
    List<URI> uriList = new ArrayList<URI>();
    
    // if the next query is join, 
    // set the partition number for the current logicalUnit
    LogicalQueryUnit nextQueryUnit = logicalUnit.getNextQuery();
    if (nextQueryUnit != null &&
        nextQueryUnit.getStoreTableNode().getSubNode().getType() == ExprType.JOIN) {
      logicalUnit.getStoreTableNode().setPartitions(
          logicalUnit.getStoreTableNode().getPartitionKeys(), n);
    }

    // set the partition number for groupby and sort
    if (logicalUnit.getOutputType() == PARTITION_TYPE.HASH) {
      StoreTableNode store = logicalUnit.getStoreTableNode();
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
        store.setPartitions(keys, n);
      }
    }

    ScanNode[] scans = logicalUnit.getScanNodes();
    for (ScanNode scan : scans) {
      if (scan.isLocal()) {
        // make fetchMap
        for (WaitStatus status: qm.getWaitStatusOfLogicalUnit(
            logicalUnit.getPrevQuery(scan))) {
          int cnt = status.getInProgressStatus().getPartitionsCount();
          for (int i = 0; i < cnt; i++) {
            uriList.add(new URI(status.getInProgressStatus().
                getPartitions(i).getFileName()));
          }
        }
      } else {
        // set fragments for each scan
        if (logicalUnit.hasPrevQuery() && 
            logicalUnit.getPrevQuery(scan).getOutputType() == 
            PARTITION_TYPE.HASH) {
          files = sm.getFileSystem().listStatus(
              sm.getTablePath(scan.getTableId()));
        } else {
          files = new FileStatus[1];
          files[0] = sm.getFileSystem().getFileStatus(
              sm.getTablePath(scan.getTableId()));
        }
        for (FileStatus file : files) {
          frags = sm.split(file.getPath());
          for (Fragment f : frags) {
            // TODO: the fragment ID should be set before
            f.setId(scan.getTableId());
            fragList.add(f);
          }
        }
      }
    }

    QueryUnit[] units = split(logicalUnit, n);
    if (logicalUnit.hasPrevQuery()) {
      for (ScanNode scan : scans) {
        if (scan.isLocal()) {
          Fragment frag = new Fragment(scan.getTableId(), 
              sm.getTablePath(scan.getTableId()), 
              TCatUtil.newTableMeta(scan.getInputSchema(),StoreType.CSV), 
              0, 0);
          fragList.add(frag);
        }
        switch (logicalUnit.getPrevQuery(scan).getOutputType()) {
        case BROADCAST:
          assignFetchesByBroadcast(units, uriList);
          break;
        case HASH:
          if (scan.isLocal()) {
            assignFetchesByHash(units, uriList);
          }
          assignFragmentsByHash(units, fragList);
          break;
        case LIST:
          if (scan.isLocal()) {
            assignFetchesByRoundRobin(units, uriList);
          } 
          assignFragmentsByRoundRobin(units, fragList);
          break;
        }
      }
    } else {
      assignFragmentsByRoundRobin(units, fragList);
    }
    
    logicalUnit.setQueryUnits(units);
    
    return units;
  }
  
  private QueryUnit[] split(LogicalQueryUnit logicalUnit, int n) {
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
  
  private void assignFetchesByRoundRobin(QueryUnit[] units, List<URI> uriList) { 
    int i = 0;
    for (URI uri : uriList) {
      units[i].addFetch(units[i].getScanNodes()[0].getTableId(), uri);
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
