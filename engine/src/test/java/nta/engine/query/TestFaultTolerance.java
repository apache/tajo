package nta.engine.query;

import com.google.common.base.Charsets;
import nta.catalog.*;
import nta.catalog.proto.CatalogProtos.*;
import nta.engine.*;
import nta.engine.ClientServiceProtos.*;
import nta.engine.cluster.QueryManager;
import nta.engine.cluster.QueryUnitStatus;
import nta.engine.planner.global.QueryUnit;
import nta.engine.planner.global.ScheduleUnit;
import nta.storage.StorageManager;
import nta.storage.StorageUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.thirdparty.guava.common.io.Files;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * @author jihoon
 */
public class TestFaultTolerance {

  public static final String TEST_DIRECTORY = "target/test-data";

  private MockupCluster cluster;
  private static Configuration conf;
  private static ExecuteQueryRequest.Builder queryRequestBuilder;
  private static Schema schema;
  private static TableMeta testMeta;
  private static TableDesc testDesc;
  private static File tableDir;
  private static String query;

  @BeforeClass
  public static void setup() throws IOException {
    query = "select * from test";
    queryRequestBuilder = ExecuteQueryRequest.newBuilder();
    queryRequestBuilder.setQuery(query);
    schema = new Schema();
    schema.addColumn("deptname", DataType.STRING);
    schema.addColumn("score", DataType.INT);
    schema.addColumn("year", DataType.INT);
    testMeta = TCatUtil.newTableMeta(schema, StoreType.CSV);

    String randomStr = UUID.randomUUID().toString();
    File dir = new File(TEST_DIRECTORY, randomStr).getAbsoluteFile();
    dir.mkdir();
    dir.deleteOnExit();
    tableDir = new File(dir, "test");
    tableDir.mkdir();
    File dataDir = new File(tableDir, "data");
    dataDir.mkdir();
    File tableFile = new File(dataDir, "test.dat");
    Writer writer = Files.newWriter(tableFile, Charsets.UTF_8);
    for (int i = 0; i < 10; i++) {
      writer.write("dept"+i+","+(i+10)+","+(i+1900)+"\n");
    }
    writer.close();
  }

  private void assertQueryResult(QueryManager qm) {
    Query q = qm.getQuery(query);
    assertEquals(MasterInterfaceProtos.QueryStatus.QUERY_FINISHED,
        qm.getQueryStatus(q));
    SubQuery subQuery = q.getSubQueryIterator().next();
    ScheduleUnit scheduleUnit = subQuery.getScheduleUnitIterator().next();
    QueryUnit[] queryUnits = scheduleUnit.getQueryUnits();
    for (QueryUnit queryUnit : queryUnits) {
      QueryUnitStatus queryUnitStatus =
          qm.getQueryUnitStatus(queryUnit.getId());
      Map<Integer, QueryUnitStatus.QueryUnitAttempt> map =
          queryUnitStatus.getAttemptMap();
      for (Map.Entry<Integer, QueryUnitStatus.QueryUnitAttempt> e
          : map.entrySet()) {
        if (e.getKey() == map.size()) {
          assertEquals(MasterInterfaceProtos.QueryStatus.QUERY_FINISHED,
              e.getValue().getStatus());
        } else {
          assertEquals(MasterInterfaceProtos.QueryStatus.QUERY_ABORTED,
              e.getValue().getStatus());
        }
      }
    }
  }

  @Test
  public void testAbort() throws Exception {
    cluster = new MockupCluster(6, 2, 2);
    conf = cluster.getConf();
    cluster.start();
    Thread.sleep(3000);
    NtaEngineMaster master = cluster.getMaster();
    testDesc = new TableDescImpl("test", schema, StoreType.CSV,
        new Options(), new Path(tableDir.getAbsolutePath()));
    StorageUtil.writeTableMeta(conf,
        new Path(tableDir.getAbsolutePath()), testMeta);
    master.getCatalog().addTable(testDesc);
    master.executeQuery(queryRequestBuilder.build());

    QueryManager qm = master.getQueryManager();
    assertQueryResult(qm);

    cluster.shutdown();
  }

  public void testDeadWorker() throws Exception {
    cluster = new MockupCluster(3, 0, 2);
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

    cluster.shutdown();
  }
}
