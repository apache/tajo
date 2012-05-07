package nta.engine;

import static org.junit.Assert.assertFalse;

import java.util.Arrays;

import nta.catalog.CatalogService;
import nta.catalog.FunctionDesc;
import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableDesc;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.FunctionType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.ipc.protocolrecords.QueryUnitRequest;
import nta.engine.parser.ParseTree;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.planner.LogicalOptimizer;
import nta.engine.planner.LogicalPlanner;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.query.QueryUnitRequestImpl;
import nta.storage.Appender;
import nta.storage.StorageManager;
import nta.storage.Tuple;
import nta.storage.VTuple;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestNtaTestingUtility {
  private NtaTestingUtility util;
  private int num = 4;
  private CatalogService catalog;
  private Configuration conf;
  private StorageManager sm;
  private QueryAnalyzer analyzer;
  private QueryContext.Factory qcFactory;
  private int tupleNum = 10000;

  @Before
  public void setUp() throws Exception {
    util = new NtaTestingUtility();
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
    FunctionDesc func = new FunctionDesc("sleep", SlowFunc.class, 
        FunctionType.AGGREGATION, DataType.STRING, new DataType [] {DataType.STRING});
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
    ScheduleUnitId sid = QueryIdFactory.newScheduleUnitId(
        QueryIdFactory.newSubQueryId(
            QueryIdFactory.newQueryId()));
    QueryUnitId qid;
    QueryContext ctx;
    ParseTree query;
    LogicalNode plan;
    QueryUnitRequest req;
    Thread.sleep(2000);
      
    sm.initTableBase(frags[0].getMeta(), "testNtaTestingUtil");
    for (int i = 0; i < 4; i++) {
      qid = QueryIdFactory.newQueryUnitId(sid);
      ctx = qcFactory.create();
      query = analyzer.parse(ctx, 
          "testNtaTestingUtil := select deptName, sleep(name) from employee group by deptName");
      plan = LogicalPlanner.createPlan(ctx, query);
      plan = LogicalOptimizer.optimize(ctx, plan);
      req = new QueryUnitRequestImpl(
          qid, Lists.newArrayList(Arrays.copyOfRange(frags, 0, splitIdx)),
          "", false, plan.toJSON());
      util.getMiniTajoCluster().getLeafServerThreads().get(i)
        .getLeafServer().requestQueryUnit(req.getProto());
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
