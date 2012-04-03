/**
 * 
 */
package nta.engine.cluster;

import static org.junit.Assert.*;

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
import nta.engine.MasterInterfaceProtos.InProgressStatus;
import nta.engine.MasterInterfaceProtos.QueryStatus;
import nta.engine.TCommonProtos.StatType;
import nta.engine.Query;
import nta.engine.QueryId;
import nta.engine.QueryIdFactory;
import nta.engine.SubQuery;
import nta.engine.SubQueryId;
import nta.engine.exception.NoSuchQueryIdException;
import nta.engine.parser.QueryBlock.FromTable;
import nta.engine.planner.global.LogicalQueryUnit;
import nta.engine.planner.global.MockQueryUnitScheduler;
import nta.engine.planner.logical.StoreTableNode;
import nta.engine.planner.logical.LogicalRootNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.query.GlobalQueryPlanner;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

/**
 * @author jihoon
 *
 */
public class TestQueryManager {
  QueryManager qm;
  
  class ProgressUpdator extends Thread {

    @Override
    public void run() {
      InProgressStatus.Builder builder = InProgressStatus.newBuilder();
      try {
        for (int i = 0; i < 3; i++) {
          Thread.sleep(1000);
          builder.setId("test"+i);
          builder.setProgress(i/3.f);
          builder.setStatus(QueryStatus.INPROGRESS);
          qm.updateProgress(QueryIdFactory.newQueryUnitId(), 
              builder.build());
        }
      } catch (InterruptedException e) {
        
      } finally {
      }
    }
    
  }

  @Before
  public void setup() {
    QueryIdFactory.reset();
    qm = new QueryManager();
  }
  
  @Test
  public void testUpdateProgress() throws InterruptedException {
    ProgressUpdator[] pu = new ProgressUpdator[2];
    for (int i = 0; i < pu.length; i++) {
      pu[i] = new ProgressUpdator();
      pu[i].start();
    }
    for (int i = 0; i < pu.length; i++) {
      pu[i].join();
    }
    assertEquals(6, qm.getAllProgresses().size());
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
    SubQueryId subId = QueryIdFactory.newSubQueryId();
    SubQuery subQuery = new SubQuery(subId);
    qm.addSubQuery(subQuery);
    GlobalQueryPlanner planner = new GlobalQueryPlanner(null, null, null);
    LogicalQueryUnit plan = planner.build(subId, root).getRoot();
    MockQueryUnitScheduler mockScheduler = new MockQueryUnitScheduler(planner, 
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
  
  private void recursiveTest(List<String> s, LogicalQueryUnit plan) 
      throws NoSuchQueryIdException {
    if (plan.hasPrevQuery()) {
      Iterator<LogicalQueryUnit> it = plan.getPrevIterator();
      while (it.hasNext()) {
        recursiveTest(s, it.next());
      }
    }
    s.addAll(qm.getAssignedWorkers(plan));
    assertTrue(qm.isFinished(plan.getId()));
    StatSet statSet = qm.getStatSet(plan.getOutputName());
    assertEquals(3, statSet.getStat(StatType.COLUMN_NUM_NDV).getValue());
    assertEquals(6, statSet.getStat(StatType.COLUMN_NUM_NULLS).getValue());
    assertEquals(9, statSet.getStat(StatType.TABLE_AVG_ROWS).getValue());
    assertEquals(12, statSet.getStat(StatType.TABLE_NUM_BLOCKS).getValue());
    assertEquals(15, statSet.getStat(StatType.TABLE_NUM_PARTITIONS).getValue());
    assertEquals(18, statSet.getStat(StatType.TABLE_NUM_ROWS).getValue());
  }
}
