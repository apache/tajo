/**
 * 
 */
package nta.engine.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableDesc;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.catalog.statistics.StatSet;
import nta.catalog.statistics.TableStat;
import nta.engine.MasterInterfaceProtos.InProgressStatusProto;
import nta.engine.MasterInterfaceProtos.QueryStatus;
import nta.engine.Query;
import nta.engine.QueryId;
import nta.engine.QueryIdFactory;
import nta.engine.SubQuery;
import nta.engine.SubQueryId;
import nta.engine.exception.NoSuchQueryIdException;
import nta.engine.parser.QueryBlock.FromTable;
import nta.engine.planner.global.MockupQueryUnitScheduler;
import nta.engine.planner.global.QueryUnit;
import nta.engine.planner.global.ScheduleUnit;
import nta.engine.planner.logical.LogicalRootNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.planner.logical.StoreTableNode;
import nta.engine.query.GlobalPlanner;
import nta.engine.query.TQueryUtil;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

/**
 * @author jihoon
 *
 */
public class TestQueryManager {
  QueryManager qm;
  
  @Before
  public void setup() {
    QueryIdFactory.reset();
    qm = new QueryManager();
  }
  
  @Test
  public void testQueryInfo() throws IOException, NoSuchQueryIdException, 
  InterruptedException {
    Schema schema = new Schema();    
    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.CSV);
    TableDesc desc = TCatUtil.newTableDesc("test", meta, new Path("/"));
    QueryIdFactory.reset();
    LogicalRootNode root = new LogicalRootNode();
    StoreTableNode store = new StoreTableNode("test");
    ScanNode scan = new ScanNode(new FromTable(desc));
    store.setSubNode(scan);
    root.setSubNode(store);
    
    QueryId qid = QueryIdFactory.newQueryId();
    Query query = new Query(qid);
    qm.addQuery(query);
    SubQueryId subId = QueryIdFactory.newSubQueryId(qid);
    SubQuery subQuery = new SubQuery(subId);
    qm.addSubQuery(subQuery);
    GlobalPlanner planner = new GlobalPlanner(null, null, null);
    ScheduleUnit plan = planner.build(subId, root).getRoot();
    MockupQueryUnitScheduler mockScheduler = new MockupQueryUnitScheduler(planner, 
        qm, plan);
    mockScheduler.run();
    List<String> s1 = new ArrayList<String>();
    recursiveTest(s1, plan);
    List<String> s2 = qm.getAssignedWorkers(query);
    assertEquals(s1.size(), s2.size());
    for (int i = 0; i < s1.size(); i++) {
      assertEquals(s1.get(i), s2.get(i));
    }
  }
  
  private void recursiveTest(List<String> s, ScheduleUnit plan) 
      throws NoSuchQueryIdException {
    if (plan.hasChildQuery()) {
      Iterator<ScheduleUnit> it = plan.getChildIterator();
      while (it.hasNext()) {
        recursiveTest(s, it.next());
      }
    }
    s.addAll(qm.getAssignedWorkers(plan));
    TableStat statSet = new TableStat();
    for (QueryUnit unit : plan.getQueryUnits()) {
      assertEquals(QueryStatus.FINISHED, unit.getInProgressStatus().getStatus());
      statSet = TQueryUtil.mergeStatSet(statSet, unit.getStats());
    }
    
//    assertEquals(3, statSet.getStat(StatType.COLUMN_NUM_NDV).getValue());
//    assertEquals(6, statSet.getStat(StatType.COLUMN_NUM_NULLS).getValue());
    assertEquals(9l, statSet.getAvgRows().longValue());
    assertEquals(12, statSet.getNumBlocks().intValue());
    assertEquals(15, statSet.getNumPartitions().intValue());
    assertEquals(18, statSet.getNumRows().longValue());
  }
}
