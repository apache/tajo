package tajo.index;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import tajo.catalog.Column;
import tajo.catalog.Schema;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableMeta;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.conf.TajoConf;
import tajo.datum.DatumFactory;
import tajo.engine.WorkerTestingUtil;
import tajo.engine.ipc.protocolrecords.Fragment;
import tajo.engine.parser.QueryBlock.SortSpec;
import tajo.engine.planner.physical.TupleComparator;
import tajo.index.bst.BSTIndex;
import tajo.index.bst.BSTIndex.BSTIndexReader;
import tajo.index.bst.BSTIndex.BSTIndexWriter;
import tajo.storage.*;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.*;

public class TestBSTIndex {
  private TajoConf conf;
  private StorageManager sm;
  private Schema schema;
  private TableMeta meta;

  private static final int TUPLE_NUM = 10000;
  private static final int LOAD_NUM = 100;
  private static final String TEST_PATH = "target/test-data/TestIndex/data";
  
  public TestBSTIndex() {
    conf = new TajoConf();
    conf.setVar(TajoConf.ConfVars.ENGINE_DATA_DIR, TEST_PATH);
    schema = new Schema();
    schema.addColumn(new Column("int", DataType.INT));
    schema.addColumn(new Column("long", DataType.LONG));
    schema.addColumn(new Column("double", DataType.DOUBLE));
    schema.addColumn(new Column("float", DataType.FLOAT));
    schema.addColumn(new Column("string", DataType.STRING));
  }

   
  @Before
  public void setUp() throws Exception {
    WorkerTestingUtil.buildTestDir(TEST_PATH);
    sm = StorageManager.get(conf, TEST_PATH);
  }
  
  @Test
  public void testFindValueInCSV() throws IOException {
    meta = TCatUtil.newTableMeta(schema, StoreType.CSV);
    
    sm.initTableBase(meta, "table1");
    Appender appender  = sm.getAppender(meta, "table1", "table1.csv");
    Tuple tuple;
    for(int i = 0 ; i < TUPLE_NUM; i ++ ) {
        tuple = new VTuple(5);
        tuple.put(0, DatumFactory.createInt(i));
        tuple.put(1, DatumFactory.createLong(i));
        tuple.put(2, DatumFactory.createDouble(i));
        tuple.put(3, DatumFactory.createFloat(i));
        tuple.put(4, DatumFactory.createString("field_"+i));
        appender.addTuple(tuple);
      }
    appender.close();
    
    appender.close();

    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);
    
    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumn("long"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumn("double"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("long", DataType.LONG));
    keySchema.addColumn(new Column("double", DataType.DOUBLE));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);
    
    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(TEST_PATH, "FindValueInCSV.idx"), BSTIndex.TWO_LEVEL_INDEX,
        keySchema, comp);    
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(1));
      keyTuple.put(1, tuple.get(2));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    scanner.close();
    
    tuple = new VTuple(keySchema.getColumnNum());
    BSTIndexReader reader = bst.getIndexReader(new Path(TEST_PATH, "FindValueInCSV.idx"), keySchema, comp);
    reader.open();
    scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    for(int i = 0 ; i < TUPLE_NUM -1 ; i ++) {
      tuple.put(0, DatumFactory.createLong(i));
      tuple.put(1, DatumFactory.createDouble(i));
      long offsets = reader.find(tuple);
      scanner.seek(offsets);
      tuple = scanner.next();
      assertTrue("seek check [" + (i) + " ," +(tuple.get(1).asLong())+ "]" , (i) == (tuple.get(1).asLong()));
      assertTrue("seek check [" + (i) + " ,"  +(tuple.get(2).asDouble())+"]" , (i) == (tuple.get(2).asDouble()));
      
      offsets = reader.next();
      if (offsets == -1) {
        continue;
      }
      scanner.seek(offsets);
      tuple = scanner.next();
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (tuple.get(0).asInt()));
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (tuple.get(1).asLong()));
    }
  }

  @Test
  public void testBuildIndexWithAppender() throws IOException {
    meta = TCatUtil.newTableMeta(schema, StoreType.CSV);

    sm.initTableBase(meta, "table1");
    FileAppender appender  = (FileAppender) sm.getAppender(meta, "table1", "table1.csv");

    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumn("long"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumn("double"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("long", DataType.LONG));
    keySchema.addColumn(new Column("double", DataType.DOUBLE));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);

    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(TEST_PATH, "BuildIndexWithAppender.idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    Tuple tuple;
    long offset;
    for(int i = 0 ; i < TUPLE_NUM; i ++ ) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt(i));
      tuple.put(1, DatumFactory.createLong(i));
      tuple.put(2, DatumFactory.createDouble(i));
      tuple.put(3, DatumFactory.createFloat(i));
      tuple.put(4, DatumFactory.createString("field_"+i));

      offset = appender.getOffset();
      appender.addTuple(tuple);
      creater.write(tuple, offset);
    }
    appender.flush();
    appender.close();

    creater.flush();
    creater.close();


    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);

    tuple = new VTuple(keySchema.getColumnNum());
    BSTIndexReader reader = bst.getIndexReader(new Path(TEST_PATH, "BuildIndexWithAppender.idx"), keySchema, comp);
    reader.open();
    SeekableScanner scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    for(int i = 0 ; i < TUPLE_NUM -1 ; i ++) {
      tuple.put(0, DatumFactory.createLong(i));
      tuple.put(1, DatumFactory.createDouble(i));
      long offsets = reader.find(tuple);
      scanner.seek(offsets);
      tuple = scanner.next();
      assertTrue("[seek check " + (i) + " ]", (i) == (tuple.get(1).asLong()));
      assertTrue("[seek check " + (i) + " ]" , (i) == (tuple.get(2).asDouble()));

      offsets = reader.next();
      if (offsets == -1) {
        continue;
      }
      scanner.seek(offsets);
      tuple = scanner.next();
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (tuple.get(0).asInt()));
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (tuple.get(1).asLong()));
    }
  }
  
  @Test
  public void testFindOmittedValueInCSV() throws IOException {
    meta = TCatUtil.newTableMeta(schema, StoreType.CSV);
    
    sm.initTableBase(meta, "table1");
    Appender appender  = sm.getAppender(meta, "table1", "table1.csv");
    Tuple tuple;
    for(int i = 0 ; i < TUPLE_NUM; i += 2 ) {
        tuple = new VTuple(5);
        tuple.put(0, DatumFactory.createInt(i));
        tuple.put(1, DatumFactory.createLong(i));
        tuple.put(2, DatumFactory.createDouble(i));
        tuple.put(3, DatumFactory.createFloat(i));
        tuple.put(4, DatumFactory.createString("field_"+i));
        appender.addTuple(tuple);
      }
    appender.close();
    
    appender.close();

    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);
    
    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumn("long"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumn("double"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("long", DataType.LONG));
    keySchema.addColumn(new Column("double", DataType.DOUBLE));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);
    
    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(TEST_PATH, "FindOmittedValueInCSV.idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(1));
      keyTuple.put(1, tuple.get(2));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    scanner.close();

    BSTIndexReader reader = bst.getIndexReader(new Path(TEST_PATH, "FindOmittedValueInCSV.idx"), keySchema, comp);
    reader.open();
    for(int i = 1 ; i < TUPLE_NUM -1 ; i+=2) {
      keyTuple.put(0, DatumFactory.createLong(i));
      keyTuple.put(1, DatumFactory.createDouble(i));
      long offsets = reader.find(keyTuple);
      assertEquals(-1, offsets);
    }
  }
  
  @Test
  public void testFindNextKeyValueInCSV() throws IOException {
    meta = TCatUtil.newTableMeta(schema, StoreType.CSV);

    sm.initTableBase(meta, "table1");
    Appender appender = sm.getAppender(meta, "table1", "table1.csv");
    Tuple tuple;
    for(int i = 0 ; i < TUPLE_NUM; i ++ ) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt(i));
      tuple.put(1, DatumFactory.createLong(i));
      tuple.put(2, DatumFactory.createDouble(i));
      tuple.put(3, DatumFactory.createFloat(i));
      tuple.put(4, DatumFactory.createString("field_"+i));
      appender.addTuple(tuple);
    }
    appender.close();

    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);
    
    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumn("int"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumn("long"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("int", DataType.INT));
    keySchema.addColumn(new Column("long", DataType.LONG));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);
    
    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(TEST_PATH, "FindNextKeyValueInCSV.idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();
    
    SeekableScanner scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(0));
      keyTuple.put(1, tuple.get(1));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    scanner.close();
    
    BSTIndexReader reader = bst.getIndexReader(new Path(TEST_PATH, "FindNextKeyValueInCSV.idx"), keySchema, comp);
    reader.open();
    scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple result;
    for(int i = 0 ; i < TUPLE_NUM -1 ; i ++) {
      keyTuple = new VTuple(2);
      keyTuple.put(0, DatumFactory.createInt(i));
      keyTuple.put(1, DatumFactory.createLong(i));
      long offsets = reader.find(keyTuple, true);
      scanner.seek(offsets);
      result = scanner.next();
      assertTrue("[seek check " + (i + 1) + " ]",
          (i + 1) == (result.get(0).asInt()));
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (result.get(1).asLong()));
      
      offsets = reader.next();
      if (offsets == -1) {
        continue;
      }
      scanner.seek(offsets);
      result = scanner.next();
      assertTrue("[seek check " + (i + 2) + " ]" , (i + 2) == (result.get(0).asLong()));
      assertTrue("[seek check " + (i + 2) + " ]" , (i + 2) == (result.get(1).asDouble()));
    }
  }
  
  @Test
  public void testFindNextKeyOmittedValueInCSV() throws IOException {
    meta = TCatUtil.newTableMeta(schema, StoreType.CSV);

    sm.initTableBase(meta, "table1");
    Appender appender = sm.getAppender(meta, "table1", "table1.csv");
    Tuple tuple;
    for(int i = 0 ; i < TUPLE_NUM; i+=2) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt(i));
      tuple.put(1, DatumFactory.createLong(i));
      tuple.put(2, DatumFactory.createDouble(i));
      tuple.put(3, DatumFactory.createFloat(i));
      tuple.put(4, DatumFactory.createString("field_"+i));
      appender.addTuple(tuple);
    }
    appender.close();

    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);
    
    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumn("int"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumn("long"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("int", DataType.INT));
    keySchema.addColumn(new Column("long", DataType.LONG));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);
    
    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(TEST_PATH, "FindNextKeyOmittedValueInCSV.idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(0));
      keyTuple.put(1, tuple.get(1));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    scanner.close();
    
    BSTIndexReader reader = bst.getIndexReader(new Path(TEST_PATH, "FindNextKeyOmittedValueInCSV.idx"),
        keySchema, comp);
    reader.open();
    scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple result;
    for(int i = 1 ; i < TUPLE_NUM -1 ; i+=2) {
      keyTuple = new VTuple(2);
      keyTuple.put(0, DatumFactory.createInt(i));
      keyTuple.put(1, DatumFactory.createLong(i));
      long offsets = reader.find(keyTuple, true);
      scanner.seek(offsets);
      result = scanner.next();
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (result.get(0).asInt()));
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (result.get(1).asLong()));
    }
  }
  
  @Test
  public void testFindValueInRaw() throws IOException {
    meta = TCatUtil.newTableMeta(schema, StoreType.RAW);
    
    sm.initTableBase(meta, "table1");
    Appender appender  = sm.getAppender(meta, "table1", "table1.csv");
    Tuple tuple;
    for(int i = 0 ; i < TUPLE_NUM; i ++ ) {
        tuple = new VTuple(5);
        tuple.put(0, DatumFactory.createInt(i));
        tuple.put(1, DatumFactory.createLong(i));
        tuple.put(2, DatumFactory.createDouble(i));
        tuple.put(3, DatumFactory.createFloat(i));
        tuple.put(4, DatumFactory.createString("field_"+i));
        appender.addTuple(tuple);
      }
    appender.close();
    
    appender.close();

    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);
    
    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumn("long"), false, false);
    sortKeys[1] = new SortSpec(schema.getColumn("double"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("long", DataType.LONG));
    keySchema.addColumn(new Column("double", DataType.DOUBLE));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);
    
    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(TEST_PATH, "FindValueInRawBSTIndex.idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(1));
      keyTuple.put(1, tuple.get(2));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    scanner.close();
    
    tuple = new VTuple(keySchema.getColumnNum());
    BSTIndexReader reader = bst.getIndexReader(new Path(TEST_PATH, "FindValueInRawBSTIndex.idx"), keySchema, comp);
    reader.open();
    scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    for(int i = 0 ; i < TUPLE_NUM -1 ; i ++) {
      tuple.put(0, DatumFactory.createLong(i));
      tuple.put(1, DatumFactory.createDouble(i));
      long offsets = reader.find(tuple, false);
      scanner.seek(offsets);
      tuple = scanner.next();
      assertTrue("[seek check " + (i) + " ]" , (i) == (tuple.get(1).asLong()));
      assertTrue("[seek check " + (i) + " ]" , (i) == (tuple.get(2).asDouble()));
    }
  }
  
  @Test
  public void testFindOmittedValueInRaw() throws IOException {
    meta = TCatUtil.newTableMeta(schema, StoreType.RAW);
    
    sm.initTableBase(meta, "table1");
    Appender appender  = sm.getAppender(meta, "table1", "table1.csv");
    Tuple tuple;
    for(int i = 0 ; i < TUPLE_NUM; i += 2 ) {
        tuple = new VTuple(5);
        tuple.put(0, DatumFactory.createInt(i));
        tuple.put(1, DatumFactory.createLong(i));
        tuple.put(2, DatumFactory.createDouble(i));
        tuple.put(3, DatumFactory.createFloat(i));
        tuple.put(4, DatumFactory.createString("field_"+i));
        appender.addTuple(tuple);
      }
    appender.close();
    
    appender.close();

    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);
    
    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumn("long"), false, false);
    sortKeys[1] = new SortSpec(schema.getColumn("double"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("long", DataType.LONG));
    keySchema.addColumn(new Column("double", DataType.DOUBLE));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);
    
    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(TEST_PATH, "FindOmittedValueInRaw.idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(1));
      keyTuple.put(1, tuple.get(2));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    scanner.close();
    
    tuple = new VTuple(keySchema.getColumnNum());
    BSTIndexReader reader = bst.getIndexReader(new Path(TEST_PATH, "FindOmittedValueInRaw.idx"),
        keySchema, comp);
    reader.open();
    for(int i = 1 ; i < TUPLE_NUM -1 ; i+=2) {
      tuple.put(0, DatumFactory.createLong(i));
      tuple.put(1, DatumFactory.createDouble(i));
      long offsets = reader.find(tuple, false);
      assertEquals(-1, offsets);
    }
  }
  
  @Test
  public void testFindNextKeyValueInRaw() throws IOException {
    meta = TCatUtil.newTableMeta(schema, StoreType.RAW);

    sm.initTableBase(meta, "table1");
    Appender appender = sm.getAppender(meta, "table1", "table1.csv");
    Tuple tuple;
    for(int i = 0 ; i < TUPLE_NUM; i ++ ) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt(i));
      tuple.put(1, DatumFactory.createLong(i));
      tuple.put(2, DatumFactory.createDouble(i));
      tuple.put(3, DatumFactory.createFloat(i));
      tuple.put(4, DatumFactory.createString("field_"+i));
      appender.addTuple(tuple);
    }
    appender.close();

    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);
    
    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumn("int"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumn("long"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("int", DataType.INT));
    keySchema.addColumn(new Column("long", DataType.LONG));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);
    
    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(TEST_PATH, "FindOmittedValueInRaw.idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(0));
      keyTuple.put(1, tuple.get(1));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    scanner.close();
    
    BSTIndexReader reader = bst.getIndexReader(new Path(TEST_PATH, "FindOmittedValueInRaw.idx"), keySchema, comp);
    reader.open();
    scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple result;
    for(int i = 0 ; i < TUPLE_NUM -1 ; i ++) {
      keyTuple = new VTuple(2);
      keyTuple.put(0, DatumFactory.createInt(i));
      keyTuple.put(1, DatumFactory.createLong(i));
      long offsets = reader.find(keyTuple, true);
      scanner.seek(offsets);
      result = scanner.next();
      assertTrue("[seek check " + (i + 1) + " ]",
          (i + 1) == (result.get(0).asInt()));
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (result.get(1).asLong()));
      
      offsets = reader.next();
      if (offsets == -1) {
        continue;
      }
      scanner.seek(offsets);
      result = scanner.next();
      assertTrue("[seek check " + (i + 2) + " ]" , (i + 2) == (result.get(0).asLong()));
      assertTrue("[seek check " + (i + 2) + " ]" , (i + 2) == (result.get(1).asDouble()));
    }
  }
  
  @Test
  public void testFindNextKeyOmittedValueInRaw() throws IOException {
    meta = TCatUtil.newTableMeta(schema, StoreType.RAW);

    sm.initTableBase(meta, "table1");
    Appender appender = sm.getAppender(meta, "table1", "table1.csv");
    Tuple tuple;
    for(int i = 0 ; i < TUPLE_NUM; i+=2) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt(i));
      tuple.put(1, DatumFactory.createLong(i));
      tuple.put(2, DatumFactory.createDouble(i));
      tuple.put(3, DatumFactory.createFloat(i));
      tuple.put(4, DatumFactory.createString("field_"+i));
      appender.addTuple(tuple);
    }
    appender.close();

    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);
    
    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumn("int"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumn("long"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("int", DataType.INT));
    keySchema.addColumn(new Column("long", DataType.LONG));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);
    
    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(TEST_PATH, "FindNextKeyOmittedValueInRaw.idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(0));
      keyTuple.put(1, tuple.get(1));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    scanner.close();
    
    BSTIndexReader reader = bst.getIndexReader(new Path(TEST_PATH, "FindNextKeyOmittedValueInRaw.idx"),
        keySchema, comp);
    reader.open();
    scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple result;
    for(int i = 1 ; i < TUPLE_NUM -1 ; i+=2) {
      keyTuple = new VTuple(2);
      keyTuple.put(0, DatumFactory.createInt(i));
      keyTuple.put(1, DatumFactory.createLong(i));
      long offsets = reader.find(keyTuple, true);
      scanner.seek(offsets);
      result = scanner.next();
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (result.get(0).asInt()));
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (result.get(1).asLong()));      
    }
  }
  
  @Test
  public void testNextInRaw() throws IOException {
    meta = TCatUtil.newTableMeta(schema, StoreType.RAW);

    sm.initTableBase(meta, "table1");
    Appender appender = sm.getAppender(meta, "table1", "table1.csv");
    Tuple tuple;
    for(int i = 0 ; i < TUPLE_NUM; i ++ ) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt(i));
      tuple.put(1, DatumFactory.createLong(i));
      tuple.put(2, DatumFactory.createDouble(i));
      tuple.put(3, DatumFactory.createFloat(i));
      tuple.put(4, DatumFactory.createString("field_"+i));
      appender.addTuple(tuple);
    }
    appender.close();

    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);
    
    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumn("int"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumn("long"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("int", DataType.INT));
    keySchema.addColumn(new Column("long", DataType.LONG));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);
    
    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(TEST_PATH, "FindNextKeyOmittedValueInRaw.idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(0));
      keyTuple.put(1, tuple.get(1));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    scanner.close();
    
    BSTIndexReader reader = bst.getIndexReader(new Path(TEST_PATH, "FindNextKeyOmittedValueInRaw.idx"),
        keySchema, comp);
    reader.open();
    scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple result;

    keyTuple = new VTuple(2);
    keyTuple.put(0, DatumFactory.createInt(0));
    keyTuple.put(1, DatumFactory.createLong(0));
    long offsets = reader.find(keyTuple);
    scanner.seek(offsets);
    result = scanner.next();
    assertTrue("[seek check " + 0 + " ]" , (0) == (result.get(0).asInt()));
    assertTrue("[seek check " + 0 + " ]" , (0) == (result.get(1).asLong()));
      
    for (int i = 1; i < TUPLE_NUM; i++) {
      offsets = reader.next();
      
      scanner.seek(offsets);
      result = scanner.next();
      assertEquals(i, result.get(0).asInt());
      assertEquals(i, result.get(1).asLong());
    }
  }

  @Test
  public void testFindMinValue() throws IOException {
    meta = TCatUtil.newTableMeta(schema, StoreType.CSV);

    sm.initTableBase(meta, "table1");
    Appender appender  = sm.getAppender(meta, "table1", "table1.csv");
    Tuple tuple;
    for(int i = 5 ; i < TUPLE_NUM + 5; i ++ ) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt(i));
      tuple.put(1, DatumFactory.createLong(i));
      tuple.put(2, DatumFactory.createDouble(i));
      tuple.put(3, DatumFactory.createFloat(i));
      tuple.put(4, DatumFactory.createString("field_"+i));
      appender.addTuple(tuple);
    }
    appender.close();

    appender.close();

    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);

    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumn("long"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumn("double"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("long", DataType.LONG));
    keySchema.addColumn(new Column("double", DataType.DOUBLE));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);

    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(TEST_PATH, "Test.idx"), BSTIndex.TWO_LEVEL_INDEX,
        keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;

      keyTuple.put(0, tuple.get(1));
      keyTuple.put(1, tuple.get(2));
      creater.write(keyTuple, offset);
    }

    creater.flush();
    creater.close();
    scanner.close();

    tuple = new VTuple(keySchema.getColumnNum());
    BSTIndexReader reader = bst.getIndexReader(new Path(TEST_PATH, "Test.idx"), keySchema, comp);
    reader.open();
    scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    tuple.put(0, DatumFactory.createLong(0));
    tuple.put(1, DatumFactory.createDouble(0));

    offset = reader.find(tuple);
    assertEquals(-1, offset);

    offset = reader.find(tuple , true);
    assertTrue(offset >= 0);
    scanner.seek(offset);
    tuple = scanner.next();
    assertEquals(5, tuple.get(1).asInt());
    assertEquals(5l, tuple.get(2).asLong());
  }

  @Test
  public void testMinMax() throws IOException {
    meta = TCatUtil.newTableMeta(schema, StoreType.RAW);

    sm.initTableBase(meta, "table1");
    Appender appender = sm.getAppender(meta, "table1", "table1.csv");
    Tuple tuple;
    for(int i = 5 ; i < TUPLE_NUM; i ++ ) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt(i));
      tuple.put(1, DatumFactory.createLong(i));
      tuple.put(2, DatumFactory.createDouble(i));
      tuple.put(3, DatumFactory.createFloat(i));
      tuple.put(4, DatumFactory.createString("field_"+i));
      appender.addTuple(tuple);
    }
    appender.close();

    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);

    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumn("int"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumn("long"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("int", DataType.INT));
    keySchema.addColumn(new Column("long", DataType.LONG));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);

    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(TEST_PATH, "FindNextKeyOmittedValueInRaw.idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;

      keyTuple.put(0, tuple.get(0));
      keyTuple.put(1, tuple.get(1));
      creater.write(keyTuple, offset);
    }

    creater.flush();
    creater.close();
    scanner.close();

    BSTIndexReader reader = bst.getIndexReader(new Path(TEST_PATH, "FindNextKeyOmittedValueInRaw.idx"),
        keySchema, comp);
    reader.open();

    Tuple min = reader.getMin();
    assertEquals(5, min.get(0).asInt());
    assertEquals(5l, min.get(0).asLong());

    Tuple max = reader.getMax();
    assertEquals(TUPLE_NUM - 1, max.get(0).asInt());
    assertEquals(TUPLE_NUM - 1, max.get(0).asLong());
  }

  private class ConcurrentAccessor implements Runnable {
    final BSTIndexReader reader;
    final Random rnd = new Random(System.currentTimeMillis());
    boolean failed = false;

    ConcurrentAccessor(BSTIndexReader reader) {
      this.reader = reader;
    }

    public boolean isFailed() {
      return this.failed;
    }

    @Override
    public void run() {
      Tuple findKey = new VTuple(2);
      int keyVal;
      for (int i = 0; i < 10000; i++) {
        keyVal = rnd.nextInt(10000);
        findKey.put(0, DatumFactory.createInt(keyVal));
        findKey.put(1, DatumFactory.createLong(keyVal));
        try {
          assertTrue(reader.find(findKey) != -1);
        } catch (Exception e) {
          e.printStackTrace();
          this.failed = true;
        }
      }
    }
  }

  @Test
  public void testConcurrentAccess() throws IOException, InterruptedException {
    meta = TCatUtil.newTableMeta(schema, StoreType.RAW);

    sm.initTableBase(meta, "table1");
    Appender appender = sm.getAppender(meta, "table1", "table1.csv");
    Tuple tuple;
    for(int i = 0 ; i < TUPLE_NUM; i ++ ) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt(i));
      tuple.put(1, DatumFactory.createLong(i));
      tuple.put(2, DatumFactory.createDouble(i));
      tuple.put(3, DatumFactory.createFloat(i));
      tuple.put(4, DatumFactory.createString("field_"+i));
      appender.addTuple(tuple);
    }
    appender.close();

    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);

    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumn("int"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumn("long"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("int", DataType.INT));
    keySchema.addColumn(new Column("long", DataType.LONG));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);

    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(TEST_PATH, "ConcurrentAccess.idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;

      keyTuple.put(0, tuple.get(0));
      keyTuple.put(1, tuple.get(1));
      creater.write(keyTuple, offset);
    }

    creater.flush();
    creater.close();
    scanner.close();

    BSTIndexReader reader = bst.getIndexReader(new Path(TEST_PATH, "ConcurrentAccess.idx"),
        keySchema, comp);
    reader.open();

    Thread [] threads = new Thread[5];
    ConcurrentAccessor [] accs = new ConcurrentAccessor[5];
    for (int i = 0; i < threads.length; i++) {
      accs[i] = new ConcurrentAccessor(reader);
      threads[i] = new Thread(accs[i]);
      threads[i].start();
    }

    for (int i = 0; i < threads.length; i++) {
      threads[i].join();
      assertFalse(accs[i].isFailed());
    }
  }
}
