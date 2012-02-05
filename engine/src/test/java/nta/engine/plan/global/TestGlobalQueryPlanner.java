package nta.engine.plan.global;

import java.io.IOException;

import nta.catalog.CatalogService;
import nta.catalog.FunctionDesc;
import nta.catalog.LocalCatalog;
import nta.catalog.Schema;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.FunctionType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.conf.NtaConf;
import nta.engine.NtaEngineMaster;
import nta.engine.NtaTestingUtility;
import nta.engine.QueryContext;
import nta.engine.exec.eval.TestEvalTree.TestSum;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.parser.QueryBlock;
import nta.engine.planner.LogicalPlanner;
import nta.engine.planner.global.GlobalQueryPlan;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.query.GlobalQueryPlanner;
import nta.storage.CSVFile2;
import nta.util.FileUtil;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * @author jihoon
 *
 */

public class TestGlobalQueryPlanner {
	
	private static NtaTestingUtility util;
	private static NtaConf conf;
	private static CatalogService catalog;
	private static GlobalQueryPlanner planner;
	private static Schema schema;
	private static NtaEngineMaster master;
	private static QueryContext.Factory factory;
	private static QueryAnalyzer analyzer;
	
	@BeforeClass
	public static void setup() throws Exception {
		util = new NtaTestingUtility();
		
	    int i, j;
		FSDataOutputStream fos;
		Path tbPath;
		
		util.startMiniCluster(3);
		master = util.getMiniNtaEngineCluster().getMaster();
		
		schema = new Schema();
		schema.addColumn("id",DataType.INT);
		schema.addColumn("age",DataType.INT);
		schema.addColumn("name",DataType.STRING);
		schema.addColumn("salary", DataType.INT);

		TableMeta meta;

		String [] tuples = {
				"1,32,hyunsik,10",
				"2,29,jihoon,20",
				"3,28,jimin,30",
				"4,24,haemi,40"
		};

		FileSystem fs = util.getMiniDFSCluster().getFileSystem();
		
		conf = new NtaConf(util.getConfiguration());
		catalog = master.getCatalog();
		FunctionDesc funcDesc = new FunctionDesc("sum", TestSum.class,
		        FunctionType.GENERAL, DataType.INT, new DataType[] { DataType.INT });
		catalog.registerFunction(funcDesc);
		    
		planner = new GlobalQueryPlanner(catalog);
		analyzer = new QueryAnalyzer(catalog);
		factory = new QueryContext.Factory(catalog);

		int tbNum = 2;
		int tupleNum;
		
		for (i = 0; i < tbNum; i++) {
			tbPath = new Path("/table"+i);
			if (fs.exists(tbPath)){
				fs.delete(tbPath, true);
			}
			fs.mkdirs(tbPath);
			fos = fs.create(new Path(tbPath, ".meta"));
			meta = new TableMetaImpl(schema, StoreType.CSV);
			meta.putOption(CSVFile2.DELIMITER, ",");			
			FileUtil.writeProto(fos, meta.getProto());
			fos.close();
			
			fos = fs.create(new Path(tbPath, "data/table.csv"));
			tupleNum = 10000000;
			for (j = 0; j < tupleNum; j++) {
				fos.writeBytes(tuples[0]+"\n");
			}
			fos.close();

			TableDesc desc = new TableDescImpl("table"+i, meta);
			desc.setPath(tbPath);
			catalog.addTable(desc);
		}
	}
	
	@AfterClass
	public static void terminate() throws IOException {
		util.shutdownMiniCluster();
	}
	
	@Test
	public void testDecompose() throws IOException, KeeperException, InterruptedException {
		catalog.updateAllTabletServingInfo(master.getOnlineServer());
		
	    QueryContext ctx = factory.create();
	    QueryBlock block = analyzer.parse(ctx, "store1 := select age, sum(salary) from table0 group by age");
	    LogicalNode logicalPlan = LogicalPlanner.createPlan(ctx, block);
		
		GlobalQueryPlan globalPlan = planner.build(logicalPlan);
		System.out.println(globalPlan.size());
	}
}
