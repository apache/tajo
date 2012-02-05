package nta.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.FunctionType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.engine.EngineTestingUtils;
import nta.engine.NtaEngineMaster;
import nta.engine.NtaTestingUtility;
import nta.engine.function.Function;
import nta.storage.CSVFile2;
import nta.util.FileUtil;
import nta.zookeeper.ZkClient;

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
		EngineTestingUtils.buildTestDir(TEST_PATH);
	}
	
	@After
	public void tearDown() throws IOException {
	}
	
	@Test
	public void testGetTable() throws Exception {
	  util = new NtaTestingUtility();
	  util.startMiniZKCluster();
	  util.startCatalogCluster();
	  CatalogService catalog = util.getMiniCatalogCluster().getCatalog();
	  
		schema1 = new Schema();
		schema1.addColumn(FieldName1, DataType.BYTE);
		schema1.addColumn(FieldName2, DataType.INT);
		schema1.addColumn(FieldName3, DataType.LONG);
		
		TableDesc meta = new TableDescImpl("table1", schema1, StoreType.MEM);
		meta.setPath(new Path("/table1"));
		
		assertFalse(catalog.existsTable("table1"));
		catalog.addTable(meta);
		assertTrue(catalog.existsTable("table1"));		
		
		TableDesc meta2 = catalog.getTableDesc("table1");
		System.out.println(meta2);
		
		catalog.deleteTable("table1");
		assertFalse(catalog.existsTable("table1"));
				
		util.shutdownCatalogCluster();
		util.shutdownMiniZKCluster();
	}
	
	@Test(expected = Throwable.class)
	public void testAddTableNoName() throws Exception {
	  util = new NtaTestingUtility();
    util.startMiniZKCluster();
    util.startCatalogCluster();
    CatalogService catalog = util.getMiniCatalogCluster().getCatalog();
    
	  schema1 = new Schema();
    schema1.addColumn(FieldName1, DataType.BYTE);
    schema1.addColumn(FieldName2, DataType.INT);
    schema1.addColumn(FieldName3, DataType.LONG);
    
	  TableMeta info = new TableMetaImpl(schema1, StoreType.CSV);
	  TableDesc desc = new TableDescImpl();
	  desc.setMeta(info);
	  
	  catalog.addTable(desc);
	  
	  util.shutdownCatalogCluster();
    util.shutdownMiniZKCluster();
	}
	
	public static class TestFunc1 extends Function {
		public TestFunc1() {
			super(					
					new Column [] {
							new Column("name", DataType.INT)
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
	
	 public static class TestFunc2 extends Function {
	    public TestFunc2() {
	      super(          
	          new Column [] {
	              new Column("name", DataType.INT),
	              new Column("bytes", DataType.BYTES)
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
	public final void testRegisterFunc() throws Exception {
	  util = new NtaTestingUtility();
    util.startMiniZKCluster();
    util.startCatalogCluster();
    CatalogService catalog = util.getMiniCatalogCluster().getCatalog();
    
		assertFalse(catalog.containFunction("test2"));
		FunctionDesc meta = new FunctionDesc("test2", TestFunc1.class, 
		    FunctionType.GENERAL, DataType.INT, 
		    new DataType [] {DataType.INT});

    catalog.registerFunction(meta);
		assertTrue(catalog.containFunction("test2", DataType.INT));
		FunctionDesc retrived = catalog.getFunction("test2", DataType.INT);

		assertEquals(retrived.getSignature(),"test2");
		assertEquals(retrived.getFuncClass(),TestFunc1.class);
		assertEquals(retrived.getFuncType(),FunctionType.GENERAL);
		
		util.shutdownCatalogCluster();
    util.shutdownMiniZKCluster();
	}

  @Test
  public final void testUnregisterFunc() throws Exception {
    util = new NtaTestingUtility();
    util.startMiniZKCluster();
    util.startCatalogCluster();
    CatalogService catalog = util.getMiniCatalogCluster().getCatalog();
    
    assertFalse(catalog
        .containFunction("test3", new DataType[] { DataType.INT }));
    FunctionDesc meta = new FunctionDesc("test3", TestFunc1.class,
        FunctionType.GENERAL, DataType.INT, new DataType[] { DataType.INT });
    catalog.registerFunction(meta);
    assertTrue(catalog.containFunction("test3", DataType.INT));
    catalog.unregisterFunction("test3", DataType.INT);
    assertFalse(catalog
        .containFunction("test3", new DataType[] { DataType.INT }));

    assertFalse(catalog.containFunction("test3", DataType.INT, DataType.BYTES));
    FunctionDesc overload = new FunctionDesc("test3", TestFunc2.class,
        FunctionType.GENERAL, DataType.INT, new DataType[] { DataType.INT,
            DataType.BYTES });
    catalog.registerFunction(overload);
    assertTrue(catalog.containFunction("test3", DataType.INT, DataType.BYTES));
    
    util.shutdownCatalogCluster();
    util.shutdownMiniZKCluster();
  }
	
	@Test
	public final void testHostsByTable() throws Exception {
	  util = new NtaTestingUtility();
	  util.startMiniCluster(3);
	  
	  //LocalCatalog local = new LocalCatalog(util.getConfiguration());
	  CatalogService local = util.getMiniNtaEngineCluster().
	      getMaster().getCatalog();
	  
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

		int tbNum = 5;
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
			local.addTable(desc);
		}
		
		local.updateAllTabletServingInfo(master.getOnlineServer());
		
		Collection<TableDesc> tables = local.getAllTableDescs();
		Iterator<TableDesc> it = tables.iterator();
		List<TabletServInfo> tabletInfoList;
		int cnt = 0;
		int len = 0;
		TableDesc tableInfo;
		FileStatus fileStatus;
		while (it.hasNext()) {
			tableInfo = it.next();
			tabletInfoList = local.getHostByTable(tableInfo.getId());
			if (tabletInfoList != null) {
				cnt++;
				len = 0;
				for (i = 0; i < tabletInfoList.size(); i++) {
					len += tabletInfoList.get(i).getTablet().getLength();
				}
				fileStatus = fs.getFileStatus(new Path(tableInfo.
				    getPath()+"/data/table.csv"));
				assertEquals(len, fileStatus.getLen());
			}
		}
		
		assertEquals(tbNum, cnt);
		
		util.shutdownMiniCluster();
	}
	
	//@Test
	// TODO - to be tested
	public void testInitializeZookeeper() throws Exception {
    ZkClient zkClient = new ZkClient(util.getConfiguration());
    assertTrue(zkClient.exists("/catalog") != null);
    
    InetSocketAddress addr = util.getMiniCatalogCluster().getCatalogServer().
        getBindAddress();
    String serverName = addr.getHostName()+":"+addr.getPort();
    assertEquals(serverName, new String(zkClient.getData("/catalog", 
        null, null)));
	}
}