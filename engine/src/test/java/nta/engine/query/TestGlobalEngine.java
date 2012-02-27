/**
 * 
 */
package nta.engine.query;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

import static org.junit.Assert.*;

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
	
	private String query = "select deptname, sum(score) from score group by deptname having sum(score) > 30"; // 9
	private static Map<String, Integer> result;

	@Before
	public void setup() throws Exception {
		util = new NtaTestingUtility();
		util.startMiniCluster(3);
		Thread.sleep(2000);
		master = util.getMiniNtaEngineCluster().getMaster();
		conf = util.getConfiguration();
		sm = new StorageManager(conf);
		
		catalog = master.getCatalog();
    result = new HashMap<String, Integer>();
		
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
	    	if (!result.containsKey("test"+(i%10))) {
	    	  result.put("test"+(i%10), i+1);
	    	} else {
	    	  int n = result.get("test"+(i%10));
	    	  result.put("test"+(i%10), n+i+1);
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
	public void test() throws Exception {
	  Thread.sleep(3000);
	  String tablename = master.executeQuery(query);
    assertNotNull(tablename);
	  LOG.info("====== Output table name is " + tablename);
	  Scanner scanner = sm.getTableScanner(tablename);
	  Tuple tuple = null;
	  String deptname;
	  while ((tuple=scanner.next()) != null) {
	    deptname = tuple.get(0).asChars();
	    assertEquals(result.get(deptname).intValue(), 
	        tuple.get(1).asInt());
	  }
	  LOG.info("result: " + result);
	}
}
