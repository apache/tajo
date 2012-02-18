package nta.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;

import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.catalog.store.DBStore;
import nta.conf.NtaConf;
import nta.engine.NtaTestingUtility;
import nta.storage.CSVFile2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Hyunsik Choi
 */
public class TestDBStore {
  private static final Log LOG = LogFactory.getLog(TestDBStore.class);  
  private static Configuration conf;
  private static NtaTestingUtility util;
  private static DBStore store;

  @BeforeClass
  public static final void setUp() throws Exception {
    conf = NtaConf.create();
    util = new NtaTestingUtility();
    File file = util.setupClusterTestBuildDir();
    conf.set(TConstants.JDBC_URI, "jdbc:derby:"+file.getAbsolutePath()+"/db");
    LOG.info("derby repository is set to "+conf.get(TConstants.JDBC_URI));
    store = new DBStore(conf);
  }

  @AfterClass
  public static final void tearDown() throws Exception {
    store.close();
  }

  @Test
  public final void testAddAndDeleteTable() throws Exception {
    Schema schema = new Schema();
    schema.addColumn("id", DataType.INT)
    .addColumn("name", DataType.STRING)
    .addColumn("age", DataType.INT)
    .addColumn("score", DataType.DOUBLE);
    
    String tableName = "addedtable";
    Options opts = new Options();
    opts.put(CSVFile2.DELIMITER, ",");
    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.CSV, opts);
    TableDesc desc = new TableDescImpl(tableName, meta, new Path("/addedtable"));
    assertFalse(store.existTable(tableName));
    store.addTable(desc);
    assertTrue(store.existTable(tableName));
    store.deleteTable(tableName);
    assertFalse(store.existTable(tableName));
  }
  
  @Test
  public final void testGetTable() throws Exception {
    Schema schema = new Schema();
    schema.addColumn("gettable.id", DataType.INT)
    .addColumn("gettable.name", DataType.STRING)
    .addColumn("gettable.age", DataType.INT)
    .addColumn("gettable.score", DataType.DOUBLE);
    
    String tableName = "gettable";
    Options opts = new Options();
    opts.put(CSVFile2.DELIMITER, ",");
    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.CSV, opts);
    TableDesc desc = new TableDescImpl(tableName, meta, new Path("/gettable"));

    store.addTable(desc);
    TableDesc retrieved = store.getTable(tableName);
    assertEquals(",", retrieved.getMeta().getOption(CSVFile2.DELIMITER));
    assertEquals(desc, retrieved);
    store.deleteTable(tableName);
  }
  
  @Test
  public final void testGetAllTableNames() throws Exception {
    Schema schema = new Schema();
    schema.addColumn("id", DataType.INT)
    .addColumn("name", DataType.STRING)
    .addColumn("age", DataType.INT)
    .addColumn("score", DataType.DOUBLE);
    
    int numTables = 5;
    for (int i = 0; i < numTables; i++) {
      String tableName = "tableA_" + i;
      TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.CSV);
      TableDesc desc = new TableDescImpl(tableName, meta, 
          new Path("/tableA_" + i));
      store.addTable(desc);
    }
    
    assertEquals(numTables, store.getAllTableNames().size());
  }
}
