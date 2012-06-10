package tajo.index;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.conf.NtaConf;
import nta.datum.DatumFactory;
import nta.engine.NConstants;
import nta.engine.WorkerTestingUtil;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.parser.QueryBlock.SortSpec;
import nta.engine.planner.physical.TupleComparator;
import nta.storage.*;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import tajo.index.bst.BSTIndex;
import tajo.index.bst.BSTIndex.BSTIndexReader;
import tajo.index.bst.BSTIndex.BSTIndexWriter;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBSTIndex {
  private NtaConf conf;
  private StorageManager sm;
  private Schema schema;
  private TableMeta meta;

  private static final int TUPLE_NUM = 10000;
  private static final int LOAD_NUM = 100;
  private static final String TEST_PATH = "target/test-data/TestIndex/data";
  
  public TestBSTIndex() {
    conf = new NtaConf();
    conf.set(NConstants.ENGINE_DATA_DIR, TEST_PATH);
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
    
    FileScanner fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset = 0;
    while (true) {
      keyTuple = new VTuple(2);
      offset = fileScanner.getNextOffset();
      tuple = fileScanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(1));
      keyTuple.put(1, tuple.get(2));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    fileScanner.close();
    
    tuple = new VTuple(keySchema.getColumnNum());
    BSTIndexReader reader = bst.getIndexReader(new Path(TEST_PATH, "FindValueInCSV.idx"), keySchema, comp);
    reader.open();
    fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    for(int i = 0 ; i < TUPLE_NUM -1 ; i ++) {
      tuple.put(0, DatumFactory.createLong(i));
      tuple.put(1, DatumFactory.createDouble(i));
      long offsets = reader.find(tuple);
      fileScanner.seek(offsets);
      tuple = fileScanner.next();
      assertTrue("[seek check " + (i) + " ]" , (i) == (tuple.get(1).asLong()));
      assertTrue("[seek check " + (i) + " ]" , (i) == (tuple.get(2).asDouble()));
      
      offsets = reader.next();
      if (offsets == -1) {
        continue;
      }
      fileScanner.seek(offsets);
      tuple = fileScanner.next();
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (tuple.get(0).asInt()));
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (tuple.get(1).asLong()));
    }
  }

  @Test
  public void testFindValueInCSVWithAppendWriter() throws IOException {
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
    BSTIndexWriter creater = bst.getIndexWriter(new Path(TEST_PATH, "FindValueInCSVWithAppendWriter.idx"),
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
    BSTIndexReader reader = bst.getIndexReader(new Path(TEST_PATH, "FindValueInCSVWithAppendWriter.idx"), keySchema, comp);
    reader.open();
    FileScanner fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    for(int i = 0 ; i < TUPLE_NUM -1 ; i ++) {
      tuple.put(0, DatumFactory.createLong(i));
      tuple.put(1, DatumFactory.createDouble(i));
      long offsets = reader.find(tuple);
      fileScanner.seek(offsets);
      tuple = fileScanner.next();
      assertTrue("[seek check " + (i) + " ]" , (i) == (tuple.get(1).asLong()));
      assertTrue("[seek check " + (i) + " ]" , (i) == (tuple.get(2).asDouble()));

      offsets = reader.next();
      if (offsets == -1) {
        continue;
      }
      fileScanner.seek(offsets);
      tuple = fileScanner.next();
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
    
    FileScanner fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset = 0;
    while (true) {
      keyTuple = new VTuple(2);
      offset = fileScanner.getNextOffset();
      tuple = fileScanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(1));
      keyTuple.put(1, tuple.get(2));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    fileScanner.close();

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
    
    FileScanner fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset = 0;
    while (true) {
      keyTuple = new VTuple(2);
      offset = fileScanner.getNextOffset();
      tuple = fileScanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(0));
      keyTuple.put(1, tuple.get(1));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    fileScanner.close();    
    
    BSTIndexReader reader = bst.getIndexReader(new Path(TEST_PATH, "FindNextKeyValueInCSV.idx"), keySchema, comp);
    reader.open();
    fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple result;
    for(int i = 0 ; i < TUPLE_NUM -1 ; i ++) {
      keyTuple = new VTuple(2);
      keyTuple.put(0, DatumFactory.createInt(i));
      keyTuple.put(1, DatumFactory.createLong(i));
      long offsets = reader.find(keyTuple, true);
      fileScanner.seek(offsets);
      result = fileScanner.next();
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (result.get(0).asInt()));
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (result.get(1).asLong()));
      
      offsets = reader.next();
      if (offsets == -1) {
        continue;
      }
      fileScanner.seek(offsets);
      result = fileScanner.next();
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
    
    FileScanner fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset = 0;
    while (true) {
      keyTuple = new VTuple(2);
      offset = fileScanner.getNextOffset();
      tuple = fileScanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(0));
      keyTuple.put(1, tuple.get(1));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    fileScanner.close();    
    
    BSTIndexReader reader = bst.getIndexReader(new Path(TEST_PATH, "FindNextKeyOmittedValueInCSV.idx"),
        keySchema, comp);
    reader.open();
    fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple result;
    for(int i = 1 ; i < TUPLE_NUM -1 ; i+=2) {
      keyTuple = new VTuple(2);
      keyTuple.put(0, DatumFactory.createInt(i));
      keyTuple.put(1, DatumFactory.createLong(i));
      long offsets = reader.find(keyTuple, true);
      fileScanner.seek(offsets);
      result = fileScanner.next();
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
    
    FileScanner fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset = 0;
    while (true) {
      keyTuple = new VTuple(2);
      offset = fileScanner.getNextOffset();
      tuple = fileScanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(1));
      keyTuple.put(1, tuple.get(2));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    fileScanner.close();
    
    tuple = new VTuple(keySchema.getColumnNum());
    BSTIndexReader reader = bst.getIndexReader(new Path(TEST_PATH, "FindValueInRawBSTIndex.idx"), keySchema, comp);
    reader.open();
    fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    for(int i = 0 ; i < TUPLE_NUM -1 ; i ++) {
      tuple.put(0, DatumFactory.createLong(i));
      tuple.put(1, DatumFactory.createDouble(i));
      long offsets = reader.find(tuple, false);
      fileScanner.seek(offsets);
      tuple = fileScanner.next();
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
    
    FileScanner fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset = 0;
    while (true) {
      keyTuple = new VTuple(2);
      offset = fileScanner.getNextOffset();
      tuple = fileScanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(1));
      keyTuple.put(1, tuple.get(2));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    fileScanner.close();
    
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
    
    FileScanner fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset = 0;
    while (true) {
      keyTuple = new VTuple(2);
      offset = fileScanner.getNextOffset();
      tuple = fileScanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(0));
      keyTuple.put(1, tuple.get(1));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    fileScanner.close();
    
    BSTIndexReader reader = bst.getIndexReader(new Path(TEST_PATH, "FindOmittedValueInRaw.idx"), keySchema, comp);
    reader.open();
    fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple result;
    for(int i = 0 ; i < TUPLE_NUM -1 ; i ++) {
      keyTuple = new VTuple(2);
      keyTuple.put(0, DatumFactory.createInt(i));
      keyTuple.put(1, DatumFactory.createLong(i));
      long offsets = reader.find(keyTuple, true);
      fileScanner.seek(offsets);
      result = fileScanner.next();
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (result.get(0).asInt()));
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (result.get(1).asLong()));
      
      offsets = reader.next();
      if (offsets == -1) {
        continue;
      }
      fileScanner.seek(offsets);
      result = fileScanner.next();
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
    
    FileScanner fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset = 0;
    while (true) {
      keyTuple = new VTuple(2);
      offset = fileScanner.getNextOffset();
      tuple = fileScanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(0));
      keyTuple.put(1, tuple.get(1));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    fileScanner.close();
    
    BSTIndexReader reader = bst.getIndexReader(new Path(TEST_PATH, "FindNextKeyOmittedValueInRaw.idx"),
        keySchema, comp);
    reader.open();
    fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple result;
    for(int i = 1 ; i < TUPLE_NUM -1 ; i+=2) {
      keyTuple = new VTuple(2);
      keyTuple.put(0, DatumFactory.createInt(i));
      keyTuple.put(1, DatumFactory.createLong(i));
      long offsets = reader.find(keyTuple, true);
      fileScanner.seek(offsets);
      result = fileScanner.next();
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
    
    FileScanner fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset = 0;
    while (true) {
      keyTuple = new VTuple(2);
      offset = fileScanner.getNextOffset();
      tuple = fileScanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(0));
      keyTuple.put(1, tuple.get(1));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    fileScanner.close();
    
    BSTIndexReader reader = bst.getIndexReader(new Path(TEST_PATH, "FindNextKeyOmittedValueInRaw.idx"),
        keySchema, comp);
    reader.open();
    fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple result;

    keyTuple = new VTuple(2);
    keyTuple.put(0, DatumFactory.createInt(0));
    keyTuple.put(1, DatumFactory.createLong(0));
    long offsets = reader.find(keyTuple);
    fileScanner.seek(offsets);
    result = fileScanner.next();
    assertTrue("[seek check " + 0 + " ]" , (0) == (result.get(0).asInt()));
    assertTrue("[seek check " + 0 + " ]" , (0) == (result.get(1).asLong()));
      
    for (int i = 1; i < TUPLE_NUM; i++) {
      offsets = reader.next();
      
      fileScanner.seek(offsets);
      result = fileScanner.next();
      assertEquals(i, result.get(0).asInt());
      assertEquals(i, result.get(1).asLong());
    }
  }

  @Test
  public void testTest() throws IOException {
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
    BSTIndexWriter creater = bst.getIndexWriter(new Path(TEST_PATH, "Test.idx"), BSTIndex.TWO_LEVEL_INDEX,
        keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    FileScanner fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset = 0;
    while (true) {
      keyTuple = new VTuple(2);
      offset = fileScanner.getNextOffset();
      tuple = fileScanner.next();
      if (tuple == null) break;

      keyTuple.put(0, tuple.get(1));
      keyTuple.put(1, tuple.get(2));
      creater.write(keyTuple, offset);
    }

    creater.flush();
    creater.close();
    fileScanner.close();

    tuple = new VTuple(keySchema.getColumnNum());
    BSTIndexReader reader = bst.getIndexReader(new Path(TEST_PATH, "Test.idx"), keySchema, comp);
    reader.open();
    fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    tuple.put(0, DatumFactory.createLong(0));
    tuple.put(1, DatumFactory.createDouble(0));
    offset = reader.find(tuple);
    fileScanner.seek(offset);
    tuple = fileScanner.next();
    System.out.println("offset:" + offset);
    System.out.println("result:" + tuple);
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

    FileScanner fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset = 0;
    while (true) {
      keyTuple = new VTuple(2);
      offset = fileScanner.getNextOffset();
      tuple = fileScanner.next();
      if (tuple == null) break;

      keyTuple.put(0, tuple.get(0));
      keyTuple.put(1, tuple.get(1));
      creater.write(keyTuple, offset);
    }

    creater.flush();
    creater.close();
    fileScanner.close();

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

  @Test
  public void testTest2() throws IOException {
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

    FileScanner fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset = 0;
    while (true) {
      keyTuple = new VTuple(2);
      offset = fileScanner.getNextOffset();
      tuple = fileScanner.next();
      if (tuple == null) break;

      keyTuple.put(0, tuple.get(0));
      keyTuple.put(1, tuple.get(1));
      creater.write(keyTuple, offset);
    }

    creater.flush();
    creater.close();
    fileScanner.close();

    BSTIndexReader reader = bst.getIndexReader(new Path(TEST_PATH, "FindNextKeyOmittedValueInRaw.idx"),
        keySchema, comp);
    reader.open();
    fileScanner  = (FileScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple result;

    keyTuple = new VTuple(2);
    keyTuple.put(0, DatumFactory.createInt(20000));
    keyTuple.put(1, DatumFactory.createLong(20000));
    long offsets = reader.find(keyTuple);
    if (offset == -1) {
      System.out.println("no exists!");
    }
    fileScanner.seek(offsets);
    result = fileScanner.next();
    System.out.println("offset: " + offsets);
    System.out.println("result: " + result);
  }
}