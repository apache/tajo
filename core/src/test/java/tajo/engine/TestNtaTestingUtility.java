package tajo.engine;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.FunctionType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.engine.ipc.protocolrecords.Fragment;
import tajo.engine.ipc.protocolrecords.QueryUnitRequest;
import tajo.engine.parser.ParseTree;
import tajo.engine.parser.QueryAnalyzer;
import tajo.engine.planner.LogicalOptimizer;
import tajo.engine.planner.LogicalPlanner;
import tajo.engine.planner.global.QueryUnit;
import tajo.engine.planner.global.QueryUnitAttempt;
import tajo.engine.planner.global.ScheduleUnit;
import tajo.engine.planner.logical.LogicalNode;
import tajo.engine.query.QueryUnitRequestImpl;
import tajo.storage.Appender;
import tajo.storage.StorageManager;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertFalse;

public class TestNtaTestingUtility {
  private TajoTestingUtility util;
  private int num = 4;
  private CatalogService catalog;
  private Configuration conf;
  private StorageManager sm;
  private QueryAnalyzer analyzer;
  private QueryContext.Factory qcFactory;
  private int tupleNum = 10000;

  @Before
  public void setUp() throws Exception {
    util = new TajoTestingUtility();
    util.startMiniCluster(num);
          
    catalog = util.getMiniTajoCluster().getMaster().getCatalog();
    conf = util.getConfiguration();
    sm = StorageManager.get(conf);
    QueryIdFactory.reset();
    analyzer = new QueryAnalyzer(catalog);
    qcFactory = new QueryContext.Factory(catalog);
    
    Schema schema = new Schema();
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("empId", DataType.INT);
    schema.addColumn("deptName", DataType.STRING);
    TableMeta employeeMeta = TCatUtil.newTableMeta(schema, StoreType.CSV);    
    sm.initTableBase(employeeMeta, "employee");

    Appender appender = sm.getAppender(employeeMeta, "employee", "employee");
    Tuple tuple = new VTuple(employeeMeta.getSchema().getColumnNum());

    for (int i = 0; i < tupleNum; i++) {
      tuple.put(new Datum[] {
          DatumFactory.createString("name_" + i),
          DatumFactory.createInt(i),
          DatumFactory.createString("dept_" + i)});
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();
    
    TableDesc desc = TCatUtil.newTableDesc("employee", employeeMeta, 
        sm.getTablePath("employee")); 
    catalog.addTable(desc);
    FunctionDesc func = new FunctionDesc("sleep", SlowFunc.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.STRING}, new DataType [] {DataType.STRING});
    catalog.registerFunction(func);
  }

  @After
  public void tearDown() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public final void test() throws Exception {
    Fragment[] frags = sm.split("employee", 40000);
    int splitIdx = (int) Math.ceil(frags.length / 2.f);
    QueryIdFactory.reset();
    QueryId queryId = QueryIdFactory.newQueryId();
    SubQueryId subQueryId = QueryIdFactory.newSubQueryId(queryId);
    ScheduleUnitId sid = QueryIdFactory.newScheduleUnitId(subQueryId);
    Query query = new Query(queryId,
        "testNtaTestingUtil := select deptName, sleep(name) from employee group by deptName");
    SubQuery subQuery = new SubQuery(subQueryId);
    ScheduleUnit scheduleUnit = new ScheduleUnit(sid);
    subQuery.addScheduleUnit(scheduleUnit);
    query.addSubQuery(subQuery);
    util.getMiniTajoCluster().getMaster().getQueryManager().addQuery(query);

    QueryUnitId qid;
    QueryContext ctx;
    ParseTree queryTree;
    LogicalNode plan;
    QueryUnitRequest req;
    Thread.sleep(2000);

    sm.initTableBase(frags[0].getMeta(), "testNtaTestingUtil");

    List<QueryUnit> queryUnits = Lists.newArrayList();
    List<QueryUnitRequest> queryUnitRequests = Lists.newArrayList();
    for (int i = 0; i < 4; i++) {
      qid = QueryIdFactory.newQueryUnitId(sid);
      ctx = qcFactory.create();
      queryTree = analyzer.parse(ctx,
          "testNtaTestingUtil := select deptName, sleep(name) from employee group by deptName");
      plan = LogicalPlanner.createPlan(ctx, queryTree);
      plan = LogicalOptimizer.optimize(ctx, plan);
      QueryUnit unit = new QueryUnit(qid);
      queryUnits.add(unit);
      QueryUnitAttempt attempt = unit.newAttempt();
      req = new QueryUnitRequestImpl(
          attempt.getId(),
          Lists.newArrayList(Arrays.copyOfRange(frags, 0, splitIdx)),
          "", false, plan.toJSON());
      queryUnitRequests.add(req);
    }
    scheduleUnit.setQueryUnits(queryUnits.toArray(new QueryUnit[queryUnits.size()]));

    for (int i = 0; i < 4; i++) {
      util.getMiniTajoCluster().getLeafServerThreads().get(i)
          .getLeafServer().requestQueryUnit(queryUnitRequests.get(i).getProto());
    }


    Thread.sleep(3000);
    LeafServer leaf0 = util.getMiniTajoCluster().getLeafServer(0);
    leaf0.shutdown("Aborted!");

    Thread.sleep(1000);
    LeafServer leaf1 = util.getMiniTajoCluster().getLeafServer(1);
    leaf1.shutdown("Aborted!");

    Thread.sleep(1000);
    LeafServer leaf2 = util.getMiniTajoCluster().getLeafServer(2);
    leaf2.shutdown("Aborted!");

    Thread.sleep(1000);
    LeafServer leaf3 = util.getMiniTajoCluster().getLeafServer(3);
    leaf3.shutdown("Aborted!");

    assertFalse(leaf0.isAlive());
    assertFalse(leaf1.isAlive());
    assertFalse(leaf2.isAlive());
    assertFalse(leaf3.isAlive());
  }
}
