package nta.engine.plan.global;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import nta.catalog.CatalogServer;
import nta.catalog.Schema;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.TabletServInfo;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.catalog.proto.CatalogProtos.TableProto;
import nta.conf.NtaConf;
import nta.engine.NtaEngineMaster;
import nta.engine.NtaTestingUtility;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.parser.RelInfo;
import nta.engine.plan.JoinType;
import nta.engine.plan.logical.JoinOp;
import nta.engine.plan.logical.LogicalPlan;
import nta.engine.plan.logical.OpType;
import nta.engine.plan.logical.RootOp;
import nta.engine.plan.logical.ScanOp;
import nta.engine.plan.logical.SelectionOp;
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
import org.junit.Test;

/**
 * 
 * @author jihoon
 *
 */

public class TestGlobalQueryPlanner {
	
	private static NtaTestingUtility util;
	private static LogicalPlan lp;
	private static NtaConf conf;
	private static CatalogServer catalog;
	private static GlobalQueryPlanner planner;
	private static Schema schema;
	private static NtaEngineMaster master;
	
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

		TableMeta meta;

		String [] tuples = {
				"1,32,hyunsik",
				"2,29,jihoon",
				"3,28,jimin",
				"4,24,haemi"
		};

		FileSystem fs = util.getMiniDFSCluster().getFileSystem();
		conf = new NtaConf(util.getConfiguration());
		catalog = new CatalogServer(conf);
		planner = new GlobalQueryPlanner(catalog);

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
	public void testBuildGenericTaskTree() throws IOException {
		ScanOp scan = new ScanOp(null);
		SelectionOp sel = new SelectionOp();
		sel.setSubOp(scan);
		JoinOp join = new JoinOp(JoinType.SEMI);
		join.setInner(sel);
		join.setOuter(new ScanOp(null));
		JoinOp join2 = new JoinOp(JoinType.SEMI);
		join2.setInner(join);
		join2.setOuter(new ScanOp(null));
		lp = new LogicalPlan(join2);
		
		RootOp root = new RootOp();
		root.setSubOp(lp.getRoot());
		GenericTaskTree tree = planner.buildGenericTaskTree(new LogicalPlan(root));
		
		Set<GenericTask> taskSet;
		Iterator<GenericTask> it;
		GenericTask task = tree.getRoot();
		assertEquals(OpType.ROOT, task.getOp().getType());
		
		taskSet = task.getNextTasks();
		assertEquals(1, taskSet.size());
		it = taskSet.iterator();
		task = it.next();
		assertEquals(OpType.JOIN, task.getOp().getType());
		
		taskSet = task.getNextTasks();
		assertEquals(2, taskSet.size());
		it = taskSet.iterator();
		task = it.next();
		assertTrue(task.getOp().getType() == OpType.JOIN 
				|| task.getOp().getType() == OpType.SCAN);
		GenericTask subTask = null;
		if (task.getOp().getType() == OpType.JOIN) {
			subTask = task;
			task = it.next();
			assertEquals(OpType.SCAN, task.getOp().getType());
		} else if (task.getOp().getType() == OpType.SCAN) {
			task = it.next();
			subTask = task;
			assertEquals(OpType.JOIN, task.getOp().getType());
		}
		
		taskSet = subTask.getNextTasks();
		assertEquals(2, taskSet.size());
		it = taskSet.iterator();
		task = it.next();
		assertTrue(task.getOp().getType() == OpType.SELECTION 
				|| task.getOp().getType() == OpType.SCAN);
		if (task.getOp().getType() == OpType.SELECTION) {
			subTask = task;
			task = it.next();
			assertEquals(OpType.SCAN, task.getOp().getType());
		} else if (task.getOp().getType() == OpType.SCAN) {
			task = it.next();
			subTask = task;
			assertEquals(OpType.SELECTION, task.getOp().getType());
		}
		
		taskSet = subTask.getNextTasks();
		assertEquals(1, taskSet.size());
		task = taskSet.iterator().next();
		assertEquals(OpType.SCAN, task.getOp().getType());
	}
	
	@Test
	public void testLocalizeSimpleOp() throws IOException, KeeperException, InterruptedException {
		catalog.updateAllTabletServingInfo(master.getOnlineServer());
		TableProto tableProto = (TableProto) FileUtil.loadProto(conf, new Path("/table0/.meta"), 
			      TableProto.getDefaultInstance());
		TableMeta meta = new TableMetaImpl(tableProto);
		ScanOp scan = new ScanOp(new RelInfo(new TableDescImpl("table0", meta)));
		LogicalPlan plan = new LogicalPlan(scan);
		GenericTaskTree genericTaskTree = planner.buildGenericTaskTree(plan);
		GenericTask[] tasks = planner.localizeSimpleOp(genericTaskTree.getRoot());
		FileSystem fs = util.getMiniDFSCluster().getFileSystem();
		FileStatus fileStat = fs.getFileStatus(new Path("/table0/data/table.csv"));
		List<Fragment> tablets;
		int len = 0;
		for (GenericTask task : tasks) {
			tablets = task.getTablets();
			for (Fragment tablet : tablets) {
				len += tablet.getLength();
			}
		}
		assertEquals(fileStat.getLen(), len);
	}
	
	@Test
	public void testDecompose() throws IOException, KeeperException, InterruptedException {
		catalog.updateAllTabletServingInfo(master.getOnlineServer());
		TableProto tableProto = (TableProto) FileUtil.loadProto(conf, new Path("/table0/.meta"), 
			      TableProto.getDefaultInstance());
		TableMeta meta = new TableMetaImpl(tableProto);
		ScanOp scan = new ScanOp(new RelInfo(new TableDescImpl("table0", meta)));
		RootOp root = new RootOp();
		root.setSubOp(scan);
		LogicalPlan plan = new LogicalPlan(root);
		GenericTaskTree taskTree = planner.localize(plan);
		List<DecomposedQuery> queries = planner.decompose(taskTree);
		List<TabletServInfo> tabletServInfo = catalog.getHostByTable("table0");
		assertEquals(tabletServInfo.size(), queries.size());

		boolean same = true;
		boolean exist = false;
		for (int i = 0; i < queries.size()-1; i++) {
			same = same && queries.get(i).getQuery().getQuery().equals(queries.get(i+1).getQuery().getQuery());
			exist = false;
			for (int j = 0; j < tabletServInfo.size(); j++) {
				if (tabletServInfo.get(j).getTablet().getId().equals(queries.get(i).getQuery().getTableName())) {
					exist = true;
					break;
				}
			}
			assertTrue(exist);
		}
		assertTrue(same);
	}
}
