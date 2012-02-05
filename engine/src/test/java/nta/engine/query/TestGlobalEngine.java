/**
 * 
 */
package nta.engine.query;

import java.io.IOException;

import nta.catalog.CatalogService;
import nta.catalog.Schema;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.datum.DatumFactory;
import nta.engine.NConstants;
import nta.engine.NtaEngineMaster;
import nta.engine.NtaTestingUtility;
import nta.storage.Appender;
import nta.storage.StorageManager;
import nta.storage.Tuple;
import nta.storage.VTuple;

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
	
	private static NtaTestingUtility util;
	private static Configuration conf;
	private static CatalogService catalog;
	private static NtaEngineMaster master;
	
	private String query = "select deptname, sum(score) from score group by deptname having sum(score) > 30"; // 9

	@Before
	public void setup() throws Exception {
		util = new NtaTestingUtility();
		util.startMiniCluster(3);
		master = util.getMiniNtaEngineCluster().getMaster();
		conf = util.getConfiguration();
		StorageManager sm = new StorageManager(conf);
		
		catalog = master.getCatalog();
//		catalog = util.getCatalog().getCatalog();
		
		Schema schema3 = new Schema();
	    schema3.addColumn("deptname", DataType.STRING);
	    schema3.addColumn("score", DataType.INT);
	    
	    TableMeta meta = new TableMetaImpl();
	    meta.setSchema(schema3);
	    meta.setStorageType(StoreType.CSV);
	    
	    Appender appender = sm.getTableAppender(meta, "score");
	    int tupleNum = 10;
	    Tuple tuple = null;
	    for (int i = 0; i < tupleNum; i++) {
	    	tuple = new VTuple(2);
	    	tuple.put(0, DatumFactory.createString("test"));
	    	tuple.put(1, DatumFactory.createInt(i+1));
	    	appender.addTuple(tuple);
	    }
	    appender.close();
	    
	    TableDesc score = new TableDescImpl("score", schema3, StoreType.CSV);
	    score.setPath(new Path(conf.get(NConstants.ENGINE_DATA_DIR), "score"));
	    catalog.addTable(score);
	}
	
	@After
	public void terminate() throws IOException {
		util.shutdownMiniCluster();
	}
	
	@Test
	public void test() throws Exception {
		master.executeQueryC(query);
//		while (true) {
//			Thread.sleep(1000);
//		}
	}
}
