/**
 * 
 */
package nta.engine.query;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nta.catalog.CatalogService;
import nta.catalog.Options;
import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.datum.DatumFactory;
import nta.engine.LeafServer;
import nta.engine.NConstants;
import nta.engine.NtaEngineMaster;
import nta.engine.NtaTestingUtility;
import nta.storage.Appender;
import nta.storage.Scanner;
import nta.storage.StorageManager;
import nta.storage.Tuple;
import nta.storage.VTuple;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author jihoon
 *
 */
public class TestGlobalEngine {
  private static Log LOG = LogFactory.getLog(TestGlobalEngine.class);
	
	private static NtaTestingUtility util;
	private static Configuration conf;
	private static CatalogService catalog;
	private static NtaEngineMaster master;
	private static StorageManager sm;
	
	private String[] query = {
	    "select deptname, sum(score) from score group by deptname having sum(score) > 30",
	    "select deptname from score"
	};
	private static Map<String, Integer> groupbyResult;
	private static Set<String> scanResult;
	
	private String tablename;

	@Before
	public void setup() throws Exception {
		util = new NtaTestingUtility();
		util.startMiniCluster(3);
		Thread.sleep(2000);
		master = util.getMiniNtaEngineCluster().getMaster();
		conf = util.getConfiguration();
		sm = new StorageManager(conf);
		
		catalog = master.getCatalog();
    groupbyResult = new HashMap<String, Integer>();
    scanResult = new HashSet<String>();
		
		Schema schema3 = new Schema();
	    schema3.addColumn("deptname", DataType.STRING);
	    schema3.addColumn("score", DataType.INT);
	    
	    TableMeta meta = TCatUtil.newTableMeta(schema3, StoreType.CSV);
	    
	    Appender appender = sm.getTableAppender(meta, "score");
	    int tupleNum = 10000000;
	    Tuple tuple = null;
	    for (int i = 0; i < tupleNum; i++) {
	    	tuple = new VTuple(2);
	    	tuple.put(0, DatumFactory.createString("test" + (i%10)));
	    	tuple.put(1, DatumFactory.createInt(i+1));
	    	appender.addTuple(tuple);
	    	scanResult.add("test"+(i%10));
	    	if (!groupbyResult.containsKey("test"+(i%10))) {
	    	  groupbyResult.put("test"+(i%10), i+1);
	    	} else {
	    	  int n = groupbyResult.get("test"+(i%10));
	    	  groupbyResult.put("test"+(i%10), n+i+1);
	    	}
	    }
	    appender.close();
	    
	    TableDesc score = new TableDescImpl("score", schema3, StoreType.CSV,
	        new Options(), 
	        new Path(conf.get(NConstants.ENGINE_DATA_DIR), "score"));
	    catalog.addTable(score);
	    
	}
	
	@After
	public void terminate() throws IOException {
		util.shutdownMiniCluster();
	}
	
	@Test
	public void testScanQuery() throws Exception {
	  String tablename = master.executeQuery(query[1]);
    assertNotNull(tablename);
    Scanner scanner = sm.getTableScanner(tablename);
    Tuple tuple = null;
    String deptname;
    while ((tuple=scanner.next()) != null) {
      deptname = tuple.get(0).asChars();
      assertTrue(scanResult.contains(deptname));
    }
	}
	
	@Test
	public void testGroupbyQuery() throws Exception {
	  String tablename = master.executeQuery(query[0]);
    assertNotNull(tablename);
	  Scanner scanner = sm.getTableScanner(tablename);
	  Tuple tuple = null;
	  String deptname;
	  while ((tuple=scanner.next()) != null) {
	    deptname = tuple.get(0).asChars();
	    assertEquals(groupbyResult.get(deptname).intValue(), 
	        tuple.get(1).asInt());
	  }
	}
	
	@Test
	public void testFaultTolerant() throws Exception {
	  Thread t1 = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(1000);
          LeafServer leaf = util.getMiniNtaEngineCluster().getLeafServer(0);
          LOG.info(">>> " + leaf.getServerName() + " will be halted!!");
          leaf.shutdown(">>> Aborted! <<<");
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
	  Thread t2 = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          tablename = master.executeQuery(query[0]);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
	  t2.start();
	  t1.start();
	  t2.join();
	  t1.join();
	  assertNotNull(tablename);
	  Scanner scanner = sm.getTableScanner(tablename);
    Tuple tuple = null;
    String deptname;
    while ((tuple=scanner.next()) != null) {
      deptname = tuple.get(0).asChars();
      assertEquals(groupbyResult.get(deptname).intValue(), 
          tuple.get(1).asInt());
    }
	}
}
