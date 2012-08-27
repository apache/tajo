/**
 * 
 */
package tajo.engine.query;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import tajo.catalog.Schema;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableMeta;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.catalog.statistics.TableStat;
import tajo.datum.DatumFactory;
import tajo.engine.NtaTestingUtility;
import tajo.storage.Appender;
import tajo.storage.StorageManager;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static org.junit.Assert.*;

/**
 * @author jihoon
 * 
 */
public class TestResultSetImpl {
  private static NtaTestingUtility util;
  private static Configuration conf;
  private static StorageManager sm;
  private static TableMeta scoreMeta;

  @BeforeClass
  public static void setup() throws Exception {
    util = new NtaTestingUtility();
    util.startMiniCluster(3);
    conf = util.getConfiguration();
    sm = new StorageManager(conf);

    Schema scoreSchema = new Schema();
    scoreSchema.addColumn("deptname", DataType.STRING);
    scoreSchema.addColumn("score", DataType.INT);
    scoreMeta = TCatUtil.newTableMeta(scoreSchema, StoreType.CSV);
    TableStat stat = new TableStat();

    Path p = sm.getTablePath("score");
    Path p2 = new Path(p, "data");
    sm.getFileSystem().mkdirs(p2);
    Appender appender = sm.getAppender(scoreMeta, 
        new Path(p2, "score"));
    int deptSize = 100;
    int tupleNum = 10000;
    Tuple tuple;
    long written = 0;
    for (int i = 0; i < tupleNum; i++) {
      tuple = new VTuple(2);
      String key = "test" + (i % deptSize);
      tuple.put(0, DatumFactory.createString(key));
      tuple.put(1, DatumFactory.createInt(i + 1));
      written += key.length() + Integer.SIZE;
      appender.addTuple(tuple);
    }
    appender.close();
    stat.setNumRows(tupleNum);
    stat.setNumBytes(written);
    stat.setAvgRows(tupleNum);
    stat.setNumBlocks(1000);
    stat.setNumPartitions(100);
    scoreMeta.setStat(stat);
    sm.writeTableMeta(sm.getTablePath("score"), scoreMeta);
  }

  @AfterClass
  public static void terminate() throws IOException {
    util.shutdownMiniCluster();
  }

  @Test
  public void test() throws IOException, SQLException {
    ResultSetImpl rs = new ResultSetImpl(conf, sm.getTablePath("score"));
    ResultSetMetaData meta = rs.getMetaData();
    assertNotNull(meta);
    Schema schema = scoreMeta.getSchema();
    assertEquals(schema.getColumnNum(), meta.getColumnCount());
    for (int i = 0; i < meta.getColumnCount(); i++) {
      assertEquals(schema.getColumn(i).getColumnName(), meta.getColumnName(i + 1));
      assertEquals(schema.getColumn(i).getTableName(), meta.getTableName(i + 1));
      assertEquals(schema.getColumn(i).getDataType().getClass().getCanonicalName(),
          meta.getColumnTypeName(i + 1));
    }

    int i = 0;
    assertTrue(rs.isBeforeFirst());
    for (; rs.next(); i++) {
      assertEquals("test"+i%100, rs.getString(1));
      assertEquals("test"+i%100, rs.getString("deptname"));
      assertEquals(i+1, rs.getInt(2));
      assertEquals(i+1, rs.getInt("score"));
    }
    assertEquals(10000, i);
    assertTrue(rs.isAfterLast());
  }
}
