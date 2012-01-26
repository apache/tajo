package nta.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.FunctionType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.conf.NtaConf;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.engine.EngineTestingUtils;
import nta.engine.NConstants;
import nta.engine.NtaEngineMaster;
import nta.engine.NtaTestingUtility;
import nta.engine.function.Function;
import nta.storage.CSVFile2;
import nta.storage.StorageManager;
import nta.util.FileUtil;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class TestCatalog {
	private NtaConf conf;
	private Catalog cat = null;
	
	static final String FieldName1="f1";
	static final String FieldName2="f2";
	static final String FieldName3="f3";	
	
	static final DataType Type1 = DataType.BYTE;
	static final DataType Type2 = DataType.INT;
	static final DataType Type3 = DataType.LONG;
	
	static final int Len2 = 10;
	static final int Len3 = 12;
	
	Column field1;
	Column field2;
	Column field3;	
	
	static final String RelName1="rel1";
	static final String RelName2="rel2";
	
	TableDescImpl rel1;
	TableDescImpl rel2;
	
	int fid1;
	int fid2;
	int fid3;
	
	int rid1;
	int rid2;
	
	Schema schema1;
	Schema schema2;
	
	static NtaTestingUtility util;
	
	final static String TEST_PATH = "/TestCatalog";
	
	@Before
	public void setUp() throws Exception {
		util = new NtaTestingUtility();		
		conf = new NtaConf(util.getConfiguration());
		EngineTestingUtils.buildTestDir(TEST_PATH);
	}
	
	@After
	public void tearDown() throws IOException {
	   cat.shutdown();
	}
	
	@Test
	public void testGetTable() throws Exception {	
	  conf.setInt(NConstants.CATALOG_MASTER_PORT, 0);
    cat = new Catalog(conf);
    cat.start();
	    
		schema1 = new Schema();
		fid1 = schema1.addColumn(FieldName1, DataType.BYTE);
		fid2 = schema1.addColumn(FieldName2, DataType.INT);
		fid3 = schema1.addColumn(FieldName3, DataType.LONG);
		
		TableDesc meta = new TableDescImpl("table1", schema1, StoreType.MEM);
		meta.setPath(new Path("/table1"));
		
		assertFalse(cat.existsTable("table1"));
		cat.addTable(meta);
		assertTrue(cat.existsTable("table1"));		
		cat.deleteTable("table1");
		assertFalse(cat.existsTable("table1"));		
		
		cat.stop("nomally shuting down");
	}
	
	@Test(expected = Throwable.class)
	public void testAddTableNoName() throws Exception {
	  cat = new Catalog(conf);
    cat.start();
    
	  schema1 = new Schema();
    fid1 = schema1.addColumn(FieldName1, DataType.BYTE);
    fid2 = schema1.addColumn(FieldName2, DataType.INT);
    fid3 = schema1.addColumn(FieldName3, DataType.LONG);
    
	  TableMeta info = new TableMetaImpl(schema1, StoreType.CSV);
	  TableDesc desc = new TableDescImpl();
	  desc.setMeta(info);
	  
	  cat.addTable(desc);
	  
	  cat.stop("nomally shuting down");
	}

/*
	@Test
	public final void testGetRelationString() throws NoSuchTableException {
		assertEquals(catalog.getTableInfo(RelName1).getRelId(),rid1);
		assertEquals(catalog.getTableInfo(RelName2).getRelId(),rid2);
	}

	@Test
	public final void testAddRelation() throws IOException {
		int rid;
		
		Schema s = new Schema();
		s.addField(new Column("age",ColumnType.INT));
		rid = catalog.addRelation("TestCatalog",s, RelationType.BASETABLE, 0, "TestCatalog");
		
		assertEquals(rid,catalog.getTableInfo(rid).getRelId());
		assertEquals("TestCatalog",catalog.getTableInfo(rid).getName());
	}

	@Test
	public final void testDelRelation() throws NoSuchTableException {
		assertNotNull(catalog.getTableInfo(RelName2));
		catalog.deleteRelation(RelName2);
//		assertNull(catalog.getRelation(RelName2));
	}
*/
	
	public static class TestFunc1 extends Function {
		public TestFunc1() {
			super(					
					new Column [] {
							new Column(1, "name", DataType.INT)
					}
			);
		}

		@Override
		public Datum invoke(Datum... datums) {
			return DatumFactory.createInt(1);
		}

		@Override
		public DataType getResType() {
			return DataType.INT;
		}
	}	

	@Test
	public final void testRegisterFunc() throws IOException {
	  cat = new Catalog(conf);
    cat.start();
	  
		assertFalse(cat.containFunction("test"));
		FunctionDesc meta = new FunctionDesc("test", TestFunc1.class, 
		    FunctionType.GENERAL, DataType.INT, 
		    new DataType [] {DataType.INT});
		cat.registerFunction(meta);
		assertTrue(cat.containFunction("test"));
		FunctionDesc retrived = cat.getFunctionMeta("test");
		assertEquals(retrived.getSignature(),"test");
		assertEquals(retrived.getFuncClass(),TestFunc1.class);
		assertEquals(retrived.getFuncType(),FunctionType.GENERAL);
		
		cat.stop("nomally shuting down");
	}

	@Test
	public final void testUnregisterFunc() throws IOException {
	  cat = new Catalog(conf);
    cat.start();
	  
		assertFalse(cat.containFunction("test"));
		FunctionDesc meta = new FunctionDesc("test", TestFunc1.class, 
        FunctionType.GENERAL, DataType.INT, 
        new DataType [] {DataType.INT});
		cat.registerFunction(meta);
		assertTrue(cat.containFunction("test"));
		cat.unregisterFunction("test");
		assertFalse(cat.containFunction("test"));
		
		cat.stop("nomally shuting down");
	}
	
	@Test
	public final void testHostsByTable() throws Exception {
	  cat = new Catalog(conf);
    cat.start();
	  
	  util.startMiniCluster(3);
	  
		int i, j;
		FSDataOutputStream fos;
		Path tbPath;
		
		NtaEngineMaster master = util.getMiniNtaEngineCluster().getMaster();
		
		Schema schema = new Schema();
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
		NtaConf conf = new NtaConf(util.getConfiguration());
		Catalog catalog = new Catalog(conf);
		StorageManager sm = new StorageManager(conf);

		int tbNum = 100;
		Random random = new Random();
		int tupleNum;
		
		for (i = 0; i < tbNum; i++) {
			tbPath = new Path(TEST_PATH+"/table"+i);
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
			tupleNum = random.nextInt(49)+100001;
			for (j = 0; j < tupleNum; j++) {
				fos.writeBytes(tuples[0]+"\n");
			}
			fos.close();

			TableDesc desc = new TableDescImpl("table"+i, meta);
			desc.setPath(tbPath);
			catalog.addTable(desc);
		}
		
		catalog.updateAllTabletServingInfo(master.getOnlineServer());
		
		Collection<TableDesc> tables = catalog.getAllTableDescs();
		Iterator<TableDesc> it = tables.iterator();
		List<TabletServInfo> tabletInfoList;
		int cnt = 0;
		int len = 0;
		TableDesc tableInfo;
		FileStatus fileStatus;
		while (it.hasNext()) {
			tableInfo = it.next();
			tabletInfoList = catalog.getHostByTable(tableInfo.getId());
			if (tabletInfoList != null) {
				cnt++;
				len = 0;
				for (i = 0; i < tabletInfoList.size(); i++) {
					len += tabletInfoList.get(i).getTablet().getLength();
				}
				fileStatus = fs.getFileStatus(new Path(tableInfo.getPath()+"/data/table.csv"));
				assertEquals(len, fileStatus.getLen());
			}
		}
		
		assertEquals(tbNum, cnt);
		
		util.shutdownMiniCluster();
		cat.stop("nomally shuting down");
	}
}
