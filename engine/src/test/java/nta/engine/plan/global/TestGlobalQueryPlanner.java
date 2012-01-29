package nta.engine.plan.global;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import nta.catalog.CatalogService;
import nta.catalog.FunctionDesc;
import nta.catalog.LocalCatalog;
import nta.catalog.Schema;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.TabletServInfo;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.FunctionType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.conf.NtaConf;
import nta.engine.NtaEngineMaster;
import nta.engine.NtaTestingUtility;
import nta.engine.QueryContext;
import nta.engine.exec.eval.TestEvalTree.TestSum;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.parser.QueryBlock;
import nta.engine.planner.LogicalPlanner;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.JoinNode;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.planner.logical.SelectionNode;
import nta.engine.query.GlobalQueryPlanner;
import nta.storage.CSVFile2;
import nta.util.FileUtil;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;

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
		catalog = new LocalCatalog(conf);
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

//	@Test
	public void testBuildGenericTaskGraph() throws IOException {
		ScanNode scan = new ScanNode(null);
		SelectionNode sel = new SelectionNode(null);
		sel.setSubNode(scan);
		JoinNode join = new JoinNode(sel, new ScanNode(null));
		JoinNode join2 = new JoinNode(join, new ScanNode(null));
		
		GenericTaskGraph tree = planner.buildGenericTaskGraph(join2);
		
		Set<GenericTask> taskSet;
		Iterator<GenericTask> it;
		GenericTask task = tree.getRoot();
		assertEquals(ExprType.JOIN, task.getOp().getType());
		
		taskSet = task.getNextTasks();
		assertEquals(2, taskSet.size());
		it = taskSet.iterator();
		task = it.next();
		assertTrue(task.getOp().getType() == ExprType.JOIN 
				|| task.getOp().getType() == ExprType.SCAN);
		GenericTask subTask = null;
		if (task.getOp().getType() == ExprType.JOIN) {
			subTask = task;
			task = it.next();
			assertEquals(ExprType.SCAN, task.getOp().getType());
		} else if (task.getOp().getType() == ExprType.SCAN) {
			task = it.next();
			subTask = task;
			assertEquals(ExprType.JOIN, task.getOp().getType());
		}
		
		taskSet = subTask.getNextTasks();
		assertEquals(2, taskSet.size());
		it = taskSet.iterator();
		task = it.next();
		assertTrue(task.getOp().getType() == ExprType.SELECTION 
				|| task.getOp().getType() == ExprType.SCAN);
		if (task.getOp().getType() == ExprType.SELECTION) {
			subTask = task;
			task = it.next();
			assertEquals(ExprType.SCAN, task.getOp().getType());
		} else if (task.getOp().getType() == ExprType.SCAN) {
			task = it.next();
			subTask = task;
			assertEquals(ExprType.SELECTION, task.getOp().getType());
		}
		
		taskSet = subTask.getNextTasks();
		assertEquals(1, taskSet.size());
		task = taskSet.iterator().next();
		assertEquals(ExprType.SCAN, task.getOp().getType());
	}
	
//	@Test
	public void testLocalizeSimpleOp() throws IOException, KeeperException, InterruptedException {
		catalog.updateAllTabletServingInfo(master.getOnlineServer());
		  
    QueryContext ctx = factory.create();
    QueryBlock block = analyzer.parse(ctx,
        "select age, sum(salary) from table0 group by age");
	  LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
		
//		TableProto tableProto = (TableProto) FileUtil.loadProto(conf, new Path("/table0/.meta"), 
//			      TableProto.getDefaultInstance());
//		TableMeta meta = new TableMetaImpl(tableProto);
//		ScanOp scan = new ScanOp(new RelInfo(new TableDescImpl("table0", meta)));
//		LogicalPlan plan = new LogicalPlan(scan);
		
		GenericTaskGraph genericTaskTree = planner.buildGenericTaskGraph(plan);
		GenericTask[] tasks = planner.localizeSimpleOp(genericTaskTree.getRoot());
		FileSystem fs = util.getMiniDFSCluster().getFileSystem();
		FileStatus fileStat = fs.getFileStatus(new Path("/table0/data/table.csv"));
		List<Fragment> tablets;
		int len = 0;
		for (GenericTask task : tasks) {
			tablets = task.getFragments();
			for (Fragment tablet : tablets) {
				len += tablet.getLength();
			}
		}
		assertEquals(fileStat.getLen(), len);
	}
	
//	@Test
	public void testDecompose() throws IOException, KeeperException, InterruptedException {
		catalog.updateAllTabletServingInfo(master.getOnlineServer());
    QueryContext ctx = factory.create();
		QueryBlock block = analyzer.parse(ctx, 
		    "select age, sum(salary) from table0 group by age");
	  LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
		
		GenericTaskGraph taskTree = planner.localize(plan);
		List<DecomposedQuery> queries = planner.decompose(taskTree);
		List<TabletServInfo> tabletServInfo = catalog.getHostByTable("table0");
		assertEquals(tabletServInfo.size(), queries.size());

		boolean same = true;
		boolean exist = false;
		for (int i = 0; i < queries.size()-1; i++) {
			same = same && queries.get(i).getQuery().getQuery().equals(queries.get(i+1).getQuery().getQuery());
			exist = false;
			for (int j = 0; j < tabletServInfo.size(); j++) {
				if (tabletServInfo.get(j).getTablet().equals(queries.get(i).getQuery().getFragments().get(0))) {
					exist = true;
					break;
				}
			}
			assertTrue(exist);
		}
		assertTrue(same);
	}
}
