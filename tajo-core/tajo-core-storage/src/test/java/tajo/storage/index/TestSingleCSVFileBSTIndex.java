package tajo.storage.index;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.conf.TajoConf;
import tajo.conf.TajoConf.ConfVars;
import tajo.datum.DatumFactory;
import tajo.storage.*;
import tajo.storage.index.bst.BSTIndex;
import tajo.storage.index.bst.BSTIndex.BSTIndexReader;
import tajo.storage.index.bst.BSTIndex.BSTIndexWriter;
import tajo.util.CommonTestingUtil;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static tajo.storage.CSVFile.CSVScanner;

public class TestSingleCSVFileBSTIndex {
  
  private TajoConf conf;
  private Schema schema;
  private TableMeta meta;
  private FileSystem fs;

  private static final int TUPLE_NUM = 10000;
  private static final int LOAD_NUM = 100;
  private static final String TEST_PATH = "target/test-data/TestSingleCSVFileBSTIndex/data";
  private Path testDir;
  
  public TestSingleCSVFileBSTIndex() {
    conf = new TajoConf();
    conf.setVar(ConfVars.ROOT_DIR, TEST_PATH);
    schema = new Schema();
    schema.addColumn(new Column("int", DataType.INT));
    schema.addColumn(new Column("long", DataType.LONG));
    schema.addColumn(new Column("double", DataType.DOUBLE));
    schema.addColumn(new Column("float", DataType.FLOAT));
    schema.addColumn(new Column("string", DataType.STRING));
  }

  @Before
  public void setUp() throws Exception {
    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    fs = testDir.getFileSystem(conf);
  }

  @Test
  public void testFindValueInSingleCSV() throws IOException {
    meta = TCatUtil.newTableMeta(schema, StoreType.CSV);

    Path tablePath = StorageUtil.concatPath(testDir, "testFindValueInSingleCSV", "table.csv");
    fs.mkdirs(tablePath.getParent());

    Appender appender = StorageManager.getAppender(conf, meta, tablePath);
    Tuple tuple;
    for (int i = 0; i < TUPLE_NUM; i++) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt(i));
      tuple.put(1, DatumFactory.createLong(i));
      tuple.put(2, DatumFactory.createDouble(i));
      tuple.put(3, DatumFactory.createFloat(i));
      tuple.put(4, DatumFactory.createString("field_" + i));
      appender.addTuple(tuple);
    }
    appender.close();

    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0,
        fileLen, null);

    SortSpec[] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumn("long"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumn("double"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("long", DataType.LONG));
    keySchema.addColumn(new Column("double", DataType.DOUBLE));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);

    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir,
        "FindValueInCSV.idx"), BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SingleFileScanner fileScanner = new CSVScanner(conf, schema, tablet);
    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = fileScanner.getNextOffset();
      tuple = fileScanner.next();
      if (tuple == null)
        break;

      keyTuple.put(0, tuple.get(1));
      keyTuple.put(1, tuple.get(2));
      creater.write(keyTuple, offset);
    }

    creater.flush();
    creater.close();
    fileScanner.close();

    tuple = new VTuple(keySchema.getColumnNum());
    BSTIndexReader reader = bst.getIndexReader(new Path(testDir,
        "FindValueInCSV.idx"), keySchema, comp);
    reader.open();
    fileScanner = new CSVScanner(conf, schema, tablet);
    for (int i = 0; i < TUPLE_NUM - 1; i++) {
      tuple.put(0, DatumFactory.createLong(i));
      tuple.put(1, DatumFactory.createDouble(i));
      long offsets = reader.find(tuple);
      fileScanner.seek(offsets);
      tuple = fileScanner.next();
      assertEquals(i,  (tuple.get(1).asLong()));
      assertEquals(i, (tuple.get(2).asDouble()) , 0.01);

      offsets = reader.next();
      if (offsets == -1) {
        continue;
      }
      fileScanner.seek(offsets);
      tuple = fileScanner.next();
      assertTrue("[seek check " + (i + 1) + " ]",
          (i + 1) == (tuple.get(0).asInt()));
      assertTrue("[seek check " + (i + 1) + " ]",
          (i + 1) == (tuple.get(1).asLong()));
    }
  }

  @Test
  public void testFindNextKeyValueInSingleCSV() throws IOException {
    meta = TCatUtil.newTableMeta(schema, StoreType.CSV);

    Path tablePath = StorageUtil.concatPath(testDir, "testFindNextKeyValueInSingleCSV",
        "table1.csv");
    fs.mkdirs(tablePath.getParent());
    Appender appender = StorageManager.getAppender(conf, meta, tablePath);
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

    FileStatus status = fs.getFileStatus(tablePath);
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen, null);
    
    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumn("int"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumn("long"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("int", DataType.INT));
    keySchema.addColumn(new Column("long", DataType.LONG));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);
    
    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir, "FindNextKeyValueInCSV.idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();
    
    SingleFileScanner fileScanner  = new CSVScanner(conf, schema, tablet);
    Tuple keyTuple;
    long offset;
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
    
    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "FindNextKeyValueInCSV.idx"), keySchema, comp);
    reader.open();
    fileScanner  = new CSVScanner(conf, schema, tablet);
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
}
