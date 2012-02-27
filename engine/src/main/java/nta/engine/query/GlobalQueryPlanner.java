package nta.engine.query;

/**
 * @author jihoon
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.parser.QueryBlock.FromTable;
import nta.engine.planner.PlannerUtil;
import nta.engine.planner.global.LogicalQueryUnit;
import nta.engine.planner.global.LogicalQueryUnit.Phase;
import nta.engine.planner.global.LogicalQueryUnitGraph;
import nta.engine.planner.global.QueryUnit;
import nta.engine.planner.logical.BinaryNode;
import nta.engine.planner.logical.CreateTableNode;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.GroupbyNode;
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
    LogicalRootNode root = (LogicalRootNode) logicalPlan;
    if (root.getSubNode().getType() != ExprType.STORE) {
      insertStore(QueryIdFactory.newLogicalQueryUnitId().toString(), 
          root);
    }
    
    // convert 2-phase plan
    LogicalNode tp = convertTo2Phase(logicalPlan);

    // make query graph
    LogicalQueryUnitGraph globalPlan = convertToGlobalPlan(subQueryId, tp);

    return globalPlan;
  }
  
  private void insertStore(String tableId, LogicalNode parent) {
    CreateTableNode store = new CreateTableNode(tableId);
    PlannerUtil.insertNode(parent, store);
  }
  
  private class TwoPhaseBuilder implements LogicalNodeVisitor {
    @Override
    public void visit(LogicalNode node) {
      if (node.getType() == ExprType.GROUP_BY) {
        PlannerUtil.transformTwoPhaseWithStore((GroupbyNode)node, 
            QueryIdFactory.newLogicalQueryUnitId().toString());
      }
    }
  }

  private LogicalNode convertTo2Phase(LogicalNode logicalPlan) {
    LogicalRootNode root = (LogicalRootNode) logicalPlan;
    root.accept(new TwoPhaseBuilder());
    return logicalPlan;
  }
  
  private LogicalQueryUnit recursiveBuildQueryUnit(List<LogicalQueryUnit> s, LogicalNode node) 
      throws IOException {
    LogicalQueryUnit unit = null, prev;
    if (node instanceof UnaryNode) {
      recursiveBuildQueryUnit(s, ((UnaryNode) node).getSubNode());
      if (node.getType() == ExprType.STORE) {
        CreateTableNode store = (CreateTableNode) node;
        LogicalQueryUnitId id = null;
        if (store.getTableName().startsWith(QueryId.PREFIX)) {
          id = new LogicalQueryUnitId(store.getTableName());
        } else {
          id = QueryIdFactory.newLogicalQueryUnitId();
        }
        Phase phase = null;

        switch (store.getSubNode().getType()) {
        case SCAN:  // store - scan
          phase = Phase.LOCAL;
          unit = new LogicalQueryUnit(id, phase);
          unit.setLogicalPlan(node);
          break;
        case GROUP_BY:  // store - groupby
        case SORT:
          UnaryNode child = (UnaryNode) store.getSubNode(); 
          if (child.getSubNode().getType() == ExprType.STORE) { // store - groupby - store
            child = (UnaryNode) child.getSubNode();
            if (child.getSubNode().getType() == ExprType.SCAN) {// store - groupby - store - scan
              // TODO: impossible case
            } else {  // store - groupby - store - groupby
              phase = Phase.MERGE;
              CreateTableNode prevStore = s.get(0).getStoreTableNode();
              TableMeta meta = TCatUtil.newTableMeta(prevStore.getOutputSchema(), 
                  StoreType.CSV);
              insertScan(store.getSubNode(), prevStore.getTableName(), meta);
            }
          } else if (child.getSubNode().getType() == ExprType.SCAN) { // store - groupby - scan
            phase = Phase.MAP;
          }
          
          unit = new LogicalQueryUnit(id, phase);
          unit.setLogicalPlan(node);
          if (s.size() > 0) {
            prev = s.get(0);
            prev.setNextQuery(unit);
            unit.addPrevQuery(prev);
          }
          break;
        case JOIN:
          break;
        }
        s.add(0, unit);
      }
    } else if (node instanceof BinaryNode) {
      recursiveBuildQueryUnit(s, ((BinaryNode) node).getLeftSubNode());
      recursiveBuildQueryUnit(s, ((BinaryNode) node).getRightSubNode());
    } else {
      
    }
    // TODO
    return s.size() > 0 ? s.get(0) : null;
  }
  
  private void insertScan(LogicalNode parent, String tableId, TableMeta meta) throws IOException {
    TableDesc desc = TCatUtil.newTableDesc(tableId, meta, sm.getTablePath(tableId));
    ScanNode newScan = new ScanNode(new FromTable(desc));
    newScan.setInputSchema(meta.getSchema());
    newScan.setOutputSchema(meta.getSchema());
    ((UnaryNode)parent).setSubNode(newScan);
  }
  
  private LogicalQueryUnitGraph convertToGlobalPlan(SubQueryId subQueryId,
      LogicalNode logicalPlan) throws IOException {
    return new LogicalQueryUnitGraph(recursiveBuildQueryUnit(
        new ArrayList<LogicalQueryUnit>(), logicalPlan));
  }

  public QueryUnit[] localize(LogicalQueryUnit unit, int n) throws IOException {
    QueryUnit[] units = split(unit, n);
    FileStatus[] files;
    Fragment[] frags;
    List<Fragment> fragList = new ArrayList<Fragment>();
    
    ScanNode[] scans = unit.getScanNodes();
    for (ScanNode scan : scans) {
      if (unit.getPhase() == Phase.MERGE) {
        files = sm.getFileSystem().listStatus(sm.getTablePath(scan.getTableId()));
      } else {
        files = new FileStatus[1];
        files[0] = sm.getFileSystem().getFileStatus(sm.getTablePath(scan.getTableId()));
        if (unit.getPhase() == Phase.MAP) {
          CreateTableNode store = unit.getStoreTableNode();
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
          store.setPartitions(keys, n);
        }
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
    
    if (unit.getPhase() == Phase.MERGE) {
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
