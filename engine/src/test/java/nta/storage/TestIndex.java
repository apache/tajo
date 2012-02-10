package nta.storage;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.conf.NtaConf;
import nta.datum.DatumFactory;
import nta.engine.EngineTestingUtils;
import nta.engine.NConstants;
import nta.engine.ipc.protocolrecords.Fragment;

import org.apache.hadoop.fs.FileStatus;
import org.junit.Before;
import org.junit.Test;

public class TestIndex {
	
	private NtaConf conf;
	private StorageManager sm;
	private Schema schema;
	private TableMeta meta;

	private static final int TUPLE_NUM = 10000;
	private static final int LOAD_NUM = 100;
	private static final String TEST_PATH = "target/test-data/TestIndex/data";
	
	@Before
	public void setUp() throws Exception {
		conf = new NtaConf();
		conf.set(NConstants.ENGINE_DATA_DIR, TEST_PATH);
		EngineTestingUtils.buildTestDir(TEST_PATH);		
		sm = StorageManager.get(conf, TEST_PATH);
		
		schema = new Schema();
		schema.addColumn(new Column("int" , DataType.INT));
		schema.addColumn(new Column("long" , DataType.LONG));
		schema.addColumn(new Column("double",DataType.DOUBLE));
		schema.addColumn(new Column("float" , DataType.FLOAT));
		
		
	}
	
	@Test
	public void testsearchEQLValueInCSVcol0() throws IOException {
		
		meta = new TableMetaImpl();
		meta.setSchema(this.schema);
		meta.setStorageType(StoreType.CSV);
			  
		sm.initTableBase(meta, "table1");
		Appender appender  = sm.getAppender(meta, "table1", "table1.csv");
		VTuple tuple = null;
		for(int i = TUPLE_NUM ; i >= 0 ; i -- ) {
			  tuple = new VTuple(4);
			  tuple.put(0, DatumFactory.createInt(i));
			  tuple.put(1, DatumFactory.createLong(i));
			  tuple.put(2, DatumFactory.createDouble(i));
			  tuple.put(3, DatumFactory.createFloat(i));
			  appender.addTuple(tuple);
		  }
		appender.close();
		
		FileStatus status = sm.listTableFiles("table1")[0];		    
		long fileLen = status.getLen();	
		
		Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);
		IndexCreater creater = new IndexCreater(conf, IndexCreater.TWO_LEVEL_INDEX);
		
		creater.setLoadNum(LOAD_NUM);
		creater.createIndex(tablet, tablet.getSchema().getColumn(0));
 
		IndexScanner scanner = new IndexScanner(conf, tablet, tablet.getSchema().getColumn(0));
		FileScanner fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
	
		for(int i = 0 ; i < TUPLE_NUM ; i ++) {
			long[] offsets = scanner.seekEQL(DatumFactory.createInt(i));
			fileScanner.seek(offsets[0]);
			tuple = (VTuple) fileScanner.next();
			assertTrue("[seek check " + i + " ]" , i == (tuple.get(0).asInt()));
		}
	}
	
	@Test
	public void testsearchGTHValueInCSVcol1() throws IOException {
		
		meta = new TableMetaImpl();
		meta.setSchema(this.schema);
		meta.setStorageType(StoreType.CSV);
			  
		sm.initTableBase(meta, "table1");
		Appender appender  = sm.getAppender(meta, "table1", "table1.csv");
		VTuple tuple = null;
		for(int i = 0 ; i < TUPLE_NUM; i ++ ) {
			  tuple = new VTuple(4);
			  tuple.put(0, DatumFactory.createInt(i));
			  tuple.put(1, DatumFactory.createLong(i));
			  tuple.put(2, DatumFactory.createDouble(i));
			  tuple.put(3, DatumFactory.createFloat(i));
			  appender.addTuple(tuple);
		  }
		appender.close();
		
		FileStatus status = sm.listTableFiles("table1")[0];		    
		long fileLen = status.getLen();	
		
		Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);
		IndexCreater creater = new IndexCreater(conf, IndexCreater.TWO_LEVEL_INDEX);
		
		creater.setLoadNum(LOAD_NUM);
		creater.createIndex(tablet, tablet.getSchema().getColumn(1));
 
		IndexScanner scanner = new IndexScanner(conf, tablet, tablet.getSchema().getColumn(1));
		FileScanner fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
		for(int i = 0 ; i < TUPLE_NUM -1 ; i ++) {
			long[] offsets = scanner.seekGTH(DatumFactory.createLong(i));
      fileScanner.seek(offsets[0]);
      tuple = (VTuple) fileScanner.next();
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (tuple.get(1).asLong()));
		}
	}
	
	@Test
	public void testsearchEQLValueInCSVcol2() throws IOException {
		
		meta = new TableMetaImpl();
		meta.setSchema(this.schema);
		meta.setStorageType(StoreType.CSV);
			  
		sm.initTableBase(meta, "table1");
		Appender appender  = sm.getAppender(meta, "table1", "table1.csv");
		VTuple tuple = null;
		for(int i = TUPLE_NUM - 1 ; i >= 0 ; i -- ) {
			  tuple = new VTuple(4);
			  tuple.put(0, DatumFactory.createInt(i));
			  tuple.put(1, DatumFactory.createLong(i));
			  tuple.put(2, DatumFactory.createDouble(i));
			  tuple.put(3, DatumFactory.createFloat(i));
			  appender.addTuple(tuple);
		  }
		appender.close();

		FileStatus status = sm.listTableFiles("table1")[0];		    
		long fileLen = status.getLen();	
		
		Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);
		IndexCreater creater = new IndexCreater(conf, IndexCreater.TWO_LEVEL_INDEX);
		
		creater.setLoadNum(LOAD_NUM);
		creater.createIndex(tablet, tablet.getMeta().getSchema().getColumn(2));
 
		IndexScanner scanner = new IndexScanner(conf, tablet, tablet.getMeta().getSchema().getColumn(2));
		FileScanner fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
		for(int i = 0 ; i < TUPLE_NUM ; i ++) {
			long[] offsets = scanner.seekEQL(DatumFactory.createDouble(i));
			fileScanner.seek(offsets[0]);
			tuple = (VTuple) fileScanner.next();
      assertTrue("[seek check " + i + " ]" , i  == (tuple.get(2).asDouble()));
		}
	}
	
	@Test
	public void testsearchGTHValueInCSVcol3() throws IOException {
		
		meta = new TableMetaImpl();
		meta.setSchema(this.schema);
		meta.setStorageType(StoreType.CSV);
			  
		sm.initTableBase(meta, "table1");
		Appender appender  = sm.getAppender(meta, "table1", "table1.csv");
		VTuple tuple = null;
		for(int i = 0 ; i < TUPLE_NUM ; i ++ ) {
			  tuple = new VTuple(4);
			  tuple.put(0, DatumFactory.createInt(i));
			  tuple.put(1, DatumFactory.createLong(i));
			  tuple.put(2, DatumFactory.createDouble(i));
			  tuple.put(3, DatumFactory.createFloat(i));
			  appender.addTuple(tuple);
		  }
		appender.close();

		FileStatus status = sm.listTableFiles("table1")[0];		    
		long fileLen = status.getLen();	
		
		Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);
		IndexCreater creater = new IndexCreater(conf, IndexCreater.TWO_LEVEL_INDEX);
		
		creater.setLoadNum(LOAD_NUM);
		creater.createIndex(tablet, tablet.getSchema().getColumn(3));
 
		IndexScanner scanner = new IndexScanner(conf, tablet, tablet.getSchema().getColumn(3));
		FileScanner fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
		for(int i = 0 ; i < TUPLE_NUM -1 ; i ++) {
			long[] offsets = scanner.seekGTH(DatumFactory.createFloat(i));
			fileScanner.seek(offsets[0]);
      tuple = (VTuple) fileScanner.next();
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1 ) == (tuple.get(3).asFloat()));
		}
	}
	
	@Test
	public void testRandomSearchEQLValueInCSVCol0 () throws IOException {
	  meta = new TableMetaImpl();
    meta.setSchema(this.schema);
    meta.setStorageType(StoreType.CSV);
        
    sm.initTableBase(meta, "table1");
    Appender appender  = sm.getAppender(meta, "table1" , "table1.csv");
    Random rnd  = new Random();
    int range = 300;
    
    VTuple tuple = null;
    for(int i = 0 ; i < TUPLE_NUM; i ++ ) {
        tuple = new VTuple(4);
        int randVal = rnd.nextInt(range);
        tuple.put(0, DatumFactory.createInt(randVal));
        tuple.put(1, DatumFactory.createLong(randVal));
        tuple.put(2, DatumFactory.createDouble(randVal));
        tuple.put(3, DatumFactory.createFloat(randVal));
        appender.addTuple(tuple);
      }
    appender.close();
    
    FileStatus status = sm.listTableFiles("table1")[0];       
    long fileLen = status.getLen(); 
    
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);
    IndexCreater creater = new IndexCreater(conf, IndexCreater.TWO_LEVEL_INDEX);

    creater.setLoadNum(LOAD_NUM);
    creater.createIndex(tablet, tablet.getSchema().getColumn(0));

    IndexScanner scanner = new IndexScanner(conf, tablet, tablet.getSchema().getColumn(0));
    FileScanner fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));

    for (int i = 0 ; i < range ; i ++ ) {
      long[] offsets = scanner.seekEQL(DatumFactory.createInt(i));
      if(offsets == null) {
      } else {
        for(int j = 0 ; j < offsets.length ; j ++ ) {
          fileScanner.seek(offsets[j]);
          tuple = (VTuple) fileScanner.next();
          assertTrue("randomSeekCheck [ val : " + i +
              " , offset : " + j +  "th " + offsets[j] + " ]" , 
              i == tuple.get(0).asInt() );
        }
      }
    }
	  
	}
	
	@Test
	public void testSearchEQLValueInRawCol0() throws IOException {
		meta = new TableMetaImpl();
		meta.setSchema(this.schema);
		meta.setStorageType(StoreType.RAW);
			  
		sm.initTableBase(meta, "table1");
		Appender appender  = sm.getAppender(meta, "table1", "table1.csv");
		VTuple tuple = null;
		for(int i = TUPLE_NUM - 1 ; i >= 0 ; i -- ) {
			  tuple = new VTuple(4);
			  tuple.put(0, DatumFactory.createInt(i));
			  tuple.put(1, DatumFactory.createLong(i));
			  tuple.put(2, DatumFactory.createDouble(i));
			  tuple.put(3, DatumFactory.createFloat(i));
			  appender.addTuple(tuple);
		  }
		appender.close();
	

		FileStatus status = sm.listTableFiles("table1")[0];		    
		long fileLen = status.getLen();	
		
		Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);
		IndexCreater creater = new IndexCreater(conf, IndexCreater.TWO_LEVEL_INDEX);
		
		creater.setLoadNum(LOAD_NUM);
		creater.createIndex(tablet, tablet.getSchema().getColumn(0));
 
		IndexScanner scanner = new IndexScanner(conf, tablet, tablet.getSchema().getColumn(0));
		FileScanner fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
	  
    for(int i = 0 ; i < TUPLE_NUM ; i ++) {
      long[] offsets = scanner.seekEQL(DatumFactory.createInt(i));
      fileScanner.seek(offsets[0]);
      tuple = (VTuple) fileScanner.next();
      assertTrue("[seek check " + i + " ]" , i == (tuple.get(0).asInt()));
    }
	}
	
	@Test
	public void testSearchGTHValueInRawCol1() throws IOException {
		meta = new TableMetaImpl();
		meta.setSchema(this.schema);
		meta.setStorageType(StoreType.RAW);
			  
		sm.initTableBase(meta, "table1");
		Appender appender  = sm.getAppender(meta, "table1", "table1.csv");
		VTuple tuple = null;
		for(int i = 0 ; i < TUPLE_NUM ; i ++ ) {
			  tuple = new VTuple(4);
			  tuple.put(0, DatumFactory.createInt(i));
			  tuple.put(1, DatumFactory.createLong(i));
			  tuple.put(2, DatumFactory.createDouble(i));
			  tuple.put(3, DatumFactory.createFloat(i));
			  appender.addTuple(tuple);
		  }
		appender.close();
	

		FileStatus status = sm.listTableFiles("table1")[0];		    
		long fileLen = status.getLen();	
		
		Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);
		IndexCreater creater = new IndexCreater(conf, IndexCreater.TWO_LEVEL_INDEX);
		
		creater.setLoadNum(LOAD_NUM);
		creater.createIndex(tablet, tablet.getSchema().getColumn(1));
 
		IndexScanner scanner = new IndexScanner(conf, tablet, tablet.getSchema().getColumn(1));
		FileScanner fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    for(int i = 0 ; i < TUPLE_NUM -1 ; i ++) {
      long[] offsets = scanner.seekGTH(DatumFactory.createDouble(i));
      fileScanner.seek(offsets[0]);
      tuple = (VTuple) fileScanner.next();
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (tuple.get(1).asLong()));
    }
    
	}
	
	@Test
	public void testSearchEQLValueInRawCol2() throws IOException {
		meta = new TableMetaImpl();
		meta.setSchema(this.schema);
		meta.setStorageType(StoreType.RAW);
			  
		sm.initTableBase(meta, "table1");
		Appender appender  = sm.getAppender(meta, "table1", "table1.csv");
		VTuple tuple = null;
		for(int i = TUPLE_NUM - 1 ; i >= 0 ; i -- ) {
			  tuple = new VTuple(4);
			  tuple.put(0, DatumFactory.createInt(i));
			  tuple.put(1, DatumFactory.createLong(i));
			  tuple.put(2, DatumFactory.createDouble(i));
			  tuple.put(3, DatumFactory.createFloat(i));
			  appender.addTuple(tuple);
		  }
		appender.close();
	
	
		FileStatus status = sm.listTableFiles("table1")[0];		    
		long fileLen = status.getLen();	
		
		Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);
		IndexCreater creater = new IndexCreater(conf, IndexCreater.TWO_LEVEL_INDEX);
		
		creater.setLoadNum(LOAD_NUM);
		creater.createIndex(tablet, tablet.getSchema().getColumn(2));
 
		IndexScanner scanner = new IndexScanner(conf, tablet, tablet.getSchema().getColumn(2));
		FileScanner fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    
    for(int i = 0 ; i < TUPLE_NUM ; i ++) {
      long[] offsets = scanner.seekEQL(DatumFactory.createInt(i));
      fileScanner.seek(offsets[0]);
      tuple = (VTuple) fileScanner.next();
      assertTrue("[seek check " + i + " ]" , i == (tuple.get(2).asDouble()));
    }
	}
	
	@Test
	public void testSearchGTHValueInRawCol3() throws IOException {
		meta = new TableMetaImpl();
		meta.setSchema(this.schema);
		meta.setStorageType(StoreType.RAW);
			  
		sm.initTableBase(meta, "table1");
		Appender appender  = sm.getAppender(meta, "table1", "table1.csv");
		VTuple tuple = null;
		for(int i = 0 ; i < TUPLE_NUM ; i ++ ) {
			  tuple = new VTuple(4);
			  tuple.put(0, DatumFactory.createInt(i));
			  tuple.put(1, DatumFactory.createLong(i));
			  tuple.put(2, DatumFactory.createDouble(i));
			  tuple.put(3, DatumFactory.createFloat(i));
			  appender.addTuple(tuple);
		  }
		appender.close();
	
	
		FileStatus status = sm.listTableFiles("table1")[0];		    
		long fileLen = status.getLen();	
		
		Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);
		IndexCreater creater = new IndexCreater(conf, IndexCreater.TWO_LEVEL_INDEX);
		
		creater.setLoadNum(LOAD_NUM);
		creater.createIndex(tablet, tablet.getSchema().getColumn(3));
 
		IndexScanner scanner = new IndexScanner(conf, tablet, tablet.getSchema().getColumn(3));
    FileScanner fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    for(int i = 0 ; i < TUPLE_NUM -1 ; i ++) {
      long[] offsets = scanner.seekGTH(DatumFactory.createDouble(i));
      fileScanner.seek(offsets[0]);
      tuple = (VTuple) fileScanner.next();
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (tuple.get(1).asFloat()));
    }
	}
	
	@Test
	public void testSearchRandomValueInRawCol0() throws IOException {
	  meta = new TableMetaImpl();
    meta.setSchema(this.schema);
    meta.setStorageType(StoreType.RAW);
        
    sm.initTableBase(meta, "table1");
    Appender appender  = sm.getAppender(meta, "table1", "table1.csv");
    Random rnd  = new Random();
    int range = 300;
    
    VTuple tuple = null;
    for(int i = 0 ; i < TUPLE_NUM; i ++ ) {
        tuple = new VTuple(4);
        int randVal = rnd.nextInt(range);
        tuple.put(0, DatumFactory.createInt(randVal));
        tuple.put(1, DatumFactory.createLong(randVal));
        tuple.put(2, DatumFactory.createDouble(randVal));
        tuple.put(3, DatumFactory.createFloat(randVal));
        appender.addTuple(tuple);
      }
    appender.close();
    
    FileStatus status = sm.listTableFiles("table1")[0];       
    long fileLen = status.getLen(); 
    
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);
    IndexCreater creater = new IndexCreater(conf, IndexCreater.TWO_LEVEL_INDEX);
    
    creater.setLoadNum(LOAD_NUM);
    creater.createIndex(tablet, tablet.getSchema().getColumn(0));
 
    IndexScanner scanner = new IndexScanner(conf, tablet, tablet.getSchema().getColumn(0));
    FileScanner fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    for (int i = 0 ; i < range ; i ++ ) {
      long[] offsets = scanner.seekEQL(DatumFactory.createInt(i));
      if(offsets == null) {
      } else {
        for(int j = 0 ; j < offsets.length ; j ++ ) {
          fileScanner.seek(offsets[j]);
          tuple = (VTuple) fileScanner.next();
          assertTrue("randomSeekCheck [ val : " + i +
              " , offset : " + j +  "th " + offsets[j] + " ]" , 
              i == tuple.get(0).asInt() );
        }
      }
    }
	}
	
	
	@Test
	public void testSearchEQLValueInRawManyCols() throws IOException{
		meta = new TableMetaImpl();
		meta.setSchema(this.schema);
		meta.setStorageType(StoreType.RAW);
			  
		sm.initTableBase(meta, "table1");
		Appender appender  = sm.getAppender(meta, "table1", "table1.csv");
		VTuple tuple = null;
		for(int i = TUPLE_NUM - 1 ; i >= 0 ; i -- ) {
			  tuple = new VTuple(4);
			  tuple.put(0, DatumFactory.createInt(i));
			  tuple.put(1, DatumFactory.createLong(i));
			  tuple.put(2, DatumFactory.createDouble(i));
			  tuple.put(3, DatumFactory.createFloat(i));
			  appender.addTuple(tuple);
		  }
		appender.close();
	
	
		FileStatus status = sm.listTableFiles("table1")[0];		    
		long fileLen = status.getLen();	
		
		Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);
		IndexCreater creater = new IndexCreater(conf, IndexCreater.TWO_LEVEL_INDEX);
		creater.createIndex(tablet, tablet.getSchema().getColumn(0));
		creater.createIndex(tablet, tablet.getSchema().getColumn(1));
		
		IndexScanner scanner_1 = new IndexScanner(conf, tablet, tablet.getSchema().getColumn(0));
		IndexScanner scanner_2 = new IndexScanner(conf, tablet, tablet.getSchema().getColumn(1));

		FileScanner fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
		for(int i = 0 ; i < TUPLE_NUM/5 ; i ++) {
			long[] offsets_1 = scanner_1.seekEQL(DatumFactory.createInt(i));
			long[] offsets_2 = scanner_2.seekEQL(DatumFactory.createLong(i));
			
			fileScanner.seek(offsets_1[0]);
			tuple = (VTuple) fileScanner.next();
      assertTrue("[seek check " + i + " ]" , i == (tuple.get(0).asInt()));
      fileScanner.seek(offsets_2[0]);
      tuple = (VTuple) fileScanner.next();
      assertTrue("[seek check " + i + " ]" , i == (tuple.get(1).asLong()));
			
		}
	}
}
