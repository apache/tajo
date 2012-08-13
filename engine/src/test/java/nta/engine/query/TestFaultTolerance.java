package nta.engine.query;

import nta.catalog.*;
import nta.catalog.proto.CatalogProtos.*;
import nta.datum.DatumFactory;
import nta.engine.*;
import nta.engine.MasterInterfaceProtos.*;
import nta.engine.ClientServiceProtos.*;
import nta.engine.cluster.QueryManager;
import nta.engine.planner.global.QueryUnit;
import nta.engine.planner.global.QueryUnitAttempt;
import nta.engine.planner.global.ScheduleUnit;
import nta.storage.*;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import sun.util.LocaleServiceProviderPool;

import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author jihoon
 */
public class TestFaultTolerance {

  public static final String TEST_DIRECTORY = "/tajo";

  private static MockupCluster cluster;
  private static Configuration conf;
  private static ExecuteQueryRequest.Builder queryRequestBuilder;
  private static Schema schema;
  private static TableMeta testMeta;
  private static TableDesc testDesc;
  private static String query;

  @BeforeClass
  public static void setup() throws Exception {
    cluster = new MockupCluster(6, 1, 4);
    conf = cluster.getConf();

    cluster.start();

    query = "select * from test";
    queryRequestBuilder = ExecuteQueryRequest.newBuilder();
    queryRequestBuilder.setQuery(query);
    schema = new Schema();
    schema.addColumn("deptname", DataType.STRING);
    schema.addColumn("score", DataType.INT);
    schema.addColumn("year", DataType.INT);
    testMeta = TCatUtil.newTableMeta(schema, StoreType.CSV);

    StorageManager sm = cluster.getMaster().getStorageManager();
    Appender appender = sm.getTableAppender(testMeta, "test");
    for (int i = 0; i < 10; i++) {
      Tuple t = new VTuple(3);
      t.put(0, DatumFactory.createString("dept"+i));
      t.put(1, DatumFactory.createInt(i+10));
      t.put(2, DatumFactory.createInt(i+1900));
      appender.addTuple(t);
    }
    appender.close();

    testDesc = new TableDescImpl("test", schema, StoreType.CSV,
        new Options(), sm.getTablePath("test"));
    cluster.getMaster().getCatalog().addTable(testDesc);

  }

  @AfterClass
  public static void terminate() throws Exception {
    cluster.shutdown();
  }

  private void assertQueryResult(QueryManager qm) {
    Query q = qm.getQuery(query);
    assertEquals(MasterInterfaceProtos.QueryStatus.QUERY_FINISHED,
        q.getStatus());
    SubQuery subQuery = q.getSubQueryIterator().next();
    ScheduleUnit scheduleUnit = subQuery.getScheduleUnitIterator().next();
    QueryUnit[] queryUnits = scheduleUnit.getQueryUnits();
    for (QueryUnit queryUnit : queryUnits) {
      QueryStatus queryUnitStatus =
          queryUnit.getStatus();

      for (int i = 0; i <= queryUnit.getRetryCount(); i++) {
        QueryUnitAttempt attempt = queryUnit.getAttempt(i);
        if (i == queryUnit.getRetryCount()) {
          assertEquals(QueryStatus.QUERY_FINISHED,
              attempt.getStatus());
        } else {
          assertEquals(QueryStatus.QUERY_ABORTED,
              attempt.getStatus());
        }
      }
    }
  }

  @Test
  public void testAbort() throws Exception {
//    Thread.sleep(3000);
    NtaEngineMaster master = cluster.getMaster();
    master.executeQuery(queryRequestBuilder.build());

    QueryManager qm = master.getQueryManager();
    assertQueryResult(qm);
  }

  public void testDeadWorker() throws Exception {
    /*cluster = new MockupCluster(3, 0, 2);
    conf = cluster.getConf();
    cluster.start();
    NtaEngineMaster master = cluster.getMaster();
    testDesc = new TableDescImpl("test", schema, StoreType.CSV,
        new Options(), new Path(tableDir.getAbsolutePath()));
    StorageUtil.writeTableMeta(conf,
        new Path(tableDir.getAbsolutePath()), testMeta);
    master.getCatalog().addTable(testDesc);
    master.executeQuery(queryRequestBuilder.build());

    QueryManager qm = master.getQueryManager();
    assertQueryResult(qm);

    cluster.shutdown();*/
  }
}
