package nta.engine.query;

/**
 * @author jihoon
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import nta.engine.planner.logical.CreateTableNode;
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

  public GlobalQueryPlanner(StorageManager sm)
      throws IOException {
    this.sm = sm;
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
      insertStore(QueryIdFactory.newLogicalQueryUnitId().toString(), root);
    }
    
    // convert 2-phase plan
    LogicalNode tp = convertTo2Phase(logicalPlan);

    // make query graph
    LogicalQueryUnitGraph globalPlan = convertToGlobalPlan(subQueryId, indexNode, tp);

    return globalPlan;
  }
  
  private void insertStore(String tableId, LogicalNode parent) {
    CreateTableNode store = new CreateTableNode(tableId);
    PlannerUtil.insertNode(parent, store);
  }
  
  private void insertStore(String leftId, String rightId, LogicalNode parent) {
    CreateTableNode left = new CreateTableNode(leftId);
    CreateTableNode right = new CreateTableNode(rightId);
    PlannerUtil.insertNode(parent, left, right);
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
          CreateTableNode store = new CreateTableNode(tableId);
          PlannerUtil.insertOuterNode(node, store);
        }
        if (join.getInnerNode().getType() != ExprType.STORE) {
          tableId = QueryIdFactory.newLogicalQueryUnitId().toString();
          CreateTableNode store = new CreateTableNode(tableId);
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
  
  private Map<CreateTableNode, LogicalQueryUnit> convertMap = 
      new HashMap<CreateTableNode, LogicalQueryUnit>();
  
  private void recursiveBuildQueryUnit(LogicalNode node) 
      throws IOException {
    LogicalQueryUnit unit = null, prev = null;
    UnaryNode unaryChild;
    CreateTableNode store, prevStore;
    if (node instanceof UnaryNode) {
      recursiveBuildQueryUnit(((UnaryNode) node).getSubNode());
      
      if (node.getType() == ExprType.STORE) {
        store = (CreateTableNode) node;
        LogicalQueryUnitId id = null;
        if (store.getTableName().startsWith(QueryId.PREFIX)) {
          id = new LogicalQueryUnitId(store.getTableName());
        } else {
          id = QueryIdFactory.newLogicalQueryUnitId();
        }
        unit = new LogicalQueryUnit(id);

        switch (store.getSubNode().getType()) {
        case SCAN:  // store - scan
          unit.setInputType(PARTITION_TYPE.LIST);
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
              unit.setInputType(PARTITION_TYPE.HASH);
              unit.setOutputType(PARTITION_TYPE.LIST);
              prevStore = (CreateTableNode) unaryChild;
              TableMeta meta = TCatUtil.newTableMeta(prevStore.getOutputSchema(), 
                  StoreType.CSV);
              insertScan(store.getSubNode(), prevStore.getTableName(), meta);
              prev = convertMap.get(prevStore);
              if (prev != null) {
                prev.setOutputType(PARTITION_TYPE.HASH);
                prev.setNextQuery(unit);
                unit.addPrevQuery(prev);
              }
            }
          } else if (unaryChild.getSubNode().getType() == ExprType.SCAN) {
            // store - groupby - scan
            unit.setInputType(PARTITION_TYPE.LIST);
            unit.setOutputType(PARTITION_TYPE.HASH);
          }
          
          unit.setLogicalPlan(node);
          break;
        case JOIN:  // store - join
          JoinNode join = (JoinNode) store.getSubNode();
          unit.setInputType(PARTITION_TYPE.HASH);
          unit.setOutputType(PARTITION_TYPE.LIST);
          
          prevStore = (CreateTableNode) join.getOuterNode();
          prev = convertMap.get(prevStore);
          if (prev != null) {
            prev.setOutputType(PARTITION_TYPE.HASH);
            prev.setNextQuery(unit);
            unit.addPrevQuery(prev);
          }
          
          CreateTableNode prevStore2 = (CreateTableNode) join.getInnerNode();
          prev = convertMap.get(prevStore2);
          if (prev != null) {
            prev.setOutputType(PARTITION_TYPE.HASH);
            prev.setNextQuery(unit);
            unit.addPrevQuery(prev);
          }

          TableMeta lmeta = TCatUtil.newTableMeta(prevStore.getOutputSchema(), 
              StoreType.CSV);
          TableMeta rmeta = TCatUtil.newTableMeta(prevStore2.getOutputSchema(), 
              StoreType.CSV);
          insertScan(join, prevStore.getTableName(), prevStore2.getTableName(), 
              lmeta, rmeta);
          
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
    newScan.setInputSchema(meta.getSchema());
    newScan.setOutputSchema(meta.getSchema());
    ((UnaryNode)parent).setSubNode(newScan);
    return parent;
  }
  
  private LogicalNode insertScan(LogicalNode parent, String leftId, String rightId, 
      TableMeta lmeta, TableMeta rmeta) throws IOException {
    TableDesc desc = TCatUtil.newTableDesc(leftId, lmeta, sm.getTablePath(leftId));
    ScanNode left = new ScanNode(new FromTable(desc));
    left.setInputSchema(lmeta.getSchema());
    left.setOutputSchema(lmeta.getSchema());
    desc = TCatUtil.newTableDesc(rightId, rmeta, sm.getTablePath(rightId));
    ScanNode right = new ScanNode(new FromTable(desc));
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

  public QueryUnit[] localize(LogicalQueryUnit logicalUnit, int n) throws IOException {
    FileStatus[] files;
    Fragment[] frags;
    List<Fragment> fragList = new ArrayList<Fragment>();
    
    ScanNode[] scans = logicalUnit.getScanNodes();
    for (ScanNode scan : scans) {
      if (logicalUnit.getInputType() == PARTITION_TYPE.HASH) {
        files = sm.getFileSystem().listStatus(sm.getTablePath(scan.getTableId()));
      } else {
        files = new FileStatus[1];
        files[0] = sm.getFileSystem().getFileStatus(sm.getTablePath(scan.getTableId()));
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
    
    Iterator<LogicalQueryUnit> it = logicalUnit.getPrevIterator();
    if (it.hasNext()) {
      LogicalQueryUnit prevLogicalUnit = it.next();
      if (((UnaryNode)prevLogicalUnit.getLogicalPlan()).getSubNode().getType() == 
          ExprType.JOIN) {
        prevLogicalUnit.getStoreTableNode().setPartitions(
            prevLogicalUnit.getStoreTableNode().getPartitionKeys(), n);
      }
    }
    
    if (logicalUnit.getOutputType() == PARTITION_TYPE.HASH) {
      CreateTableNode store = logicalUnit.getStoreTableNode();
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
    
    QueryUnit[] units = split(logicalUnit, n);
    if (logicalUnit.getInputType() == PARTITION_TYPE.HASH) {
      assignFragmentsByHash(units, fragList);
    } else {
      assignFragmentsByRoundRobin(units, fragList);
    }
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
  
  private void assignFragmentsByHash(QueryUnit[] units, List<Fragment> fragList) {
    Collection<List<Fragment>> hashed = hashFragments(fragList);
    int i = 0;
    for (List<Fragment> frags : hashed) {
      units[i++].setFragments(frags.toArray(new Fragment[frags.size()]));
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
