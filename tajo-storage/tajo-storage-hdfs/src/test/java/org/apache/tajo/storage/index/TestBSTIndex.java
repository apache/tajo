/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage.index;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.*;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.index.bst.BSTIndex;
import org.apache.tajo.storage.index.bst.BSTIndex.BSTIndexReader;
import org.apache.tajo.storage.index.bst.BSTIndex.BSTIndexWriter;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TestBSTIndex {
  private TajoConf conf;
  private Schema schema;
  private TableMeta meta;

  private static final int TUPLE_NUM = 10000;
  private static final int LOAD_NUM = 100;
  private static final String TEST_PATH = "target/test-data/TestIndex";
  private Path testDir;
  private FileSystem fs;
  private String storeType;

  public TestBSTIndex(String type) {
    this.storeType = type;
    conf = new TajoConf();
    conf.setVar(TajoConf.ConfVars.ROOT_DIR, TEST_PATH);
    schema = new Schema();
    schema.addColumn(new Column("int", Type.INT4));
    schema.addColumn(new Column("long", Type.INT8));
    schema.addColumn(new Column("double", Type.FLOAT8));
    schema.addColumn(new Column("float", Type.FLOAT4));
    schema.addColumn(new Column("string", Type.TEXT));
  }


  @Parameterized.Parameters
  public static Collection<Object[]> generateParameters() {
    return Arrays.asList(new Object[][]{
        {"CSV"},
        {"RAW"},
        {"TEXT"}
    });
  }

  @Before
  public void setUp() throws Exception {
    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    fs = testDir.getFileSystem(conf);
  }

  @Test
  public void testFindValue() throws IOException {
    meta = CatalogUtil.newTableMeta(storeType);

    Path tablePath = new Path(testDir, "testFindValue_" + storeType);
    Appender appender = ((FileTablespace) TableSpaceManager.getFileStorageManager(conf)).getAppender(meta, schema, tablePath);
    appender.init();
    Tuple tuple;
    for (int i = 0; i < TUPLE_NUM; i++) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt4(i));
      tuple.put(1, DatumFactory.createInt8(i));
      tuple.put(2, DatumFactory.createFloat8(i));
      tuple.put(3, DatumFactory.createFloat4(i));
      tuple.put(4, DatumFactory.createText("field_" + i));
      appender.addTuple(tuple);
    }
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    long fileLen = status.getLen();
    FileFragment tablet = new FileFragment("table1_1", status.getPath(), 0, fileLen);

    SortSpec[] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumn("long"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumn("double"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("long", Type.INT8));
    keySchema.addColumn(new Column("double", Type.FLOAT8));

    BaseTupleComparator comp = new BaseTupleComparator(keySchema, sortKeys);

    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir, "testFindValue_" + storeType + ".idx"),
        BSTIndex.TWO_LEVEL_INDEX,
        keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner = TableSpaceManager.getSeekableScanner(conf, meta, schema, tablet, schema);
    scanner.init();

    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;

      keyTuple.put(0, tuple.asDatum(1));
      keyTuple.put(1, tuple.asDatum(2));
      creater.write(keyTuple, offset);
    }

    creater.flush();
    creater.close();
    scanner.close();

    tuple = new VTuple(keySchema.size());
    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "testFindValue_" + storeType + ".idx"), keySchema, comp);
    reader.open();
    scanner = TableSpaceManager.getSeekableScanner(conf, meta, schema, tablet, schema);
    scanner.init();

    for (int i = 0; i < TUPLE_NUM - 1; i++) {
      tuple.put(0, DatumFactory.createInt8(i));
      tuple.put(1, DatumFactory.createFloat8(i));
      long offsets = reader.find(tuple);
      scanner.seek(offsets);
      tuple = scanner.next();
      assertTrue("seek check [" + (i) + " ," + (tuple.getInt8(1)) + "]", (i) == (tuple.getInt8(1)));
      assertTrue("seek check [" + (i) + " ," + (tuple.getFloat8(2)) + "]", (i) == (tuple.getFloat8(2)));

      offsets = reader.next();
      if (offsets == -1) {
        continue;
      }
      scanner.seek(offsets);
      tuple = scanner.next();
      assertTrue("[seek check " + (i + 1) + " ]", (i + 1) == (tuple.getInt4(0)));
      assertTrue("[seek check " + (i + 1) + " ]", (i + 1) == (tuple.getInt8(1)));
    }
    reader.close();
    scanner.close();
  }

  @Test
  public void testBuildIndexWithAppender() throws IOException {
    meta = CatalogUtil.newTableMeta(storeType);

    Path tablePath = new Path(testDir, "testBuildIndexWithAppender_" + storeType);
    FileAppender appender = (FileAppender) ((FileTablespace) TableSpaceManager.getFileStorageManager(conf))
        .getAppender(meta, schema, tablePath);
    appender.init();

    SortSpec[] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumn("long"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumn("double"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("long", Type.INT8));
    keySchema.addColumn(new Column("double", Type.FLOAT8));

    BaseTupleComparator comp = new BaseTupleComparator(keySchema, sortKeys);

    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir, "testBuildIndexWithAppender_" + storeType + ".idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    Tuple tuple;
    long offset;
    for (int i = 0; i < TUPLE_NUM; i++) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt4(i));
      tuple.put(1, DatumFactory.createInt8(i));
      tuple.put(2, DatumFactory.createFloat8(i));
      tuple.put(3, DatumFactory.createFloat4(i));
      tuple.put(4, DatumFactory.createText("field_" + i));

      offset = appender.getOffset();
      appender.addTuple(tuple);
      creater.write(tuple, offset);
    }
    appender.flush();
    appender.close();

    creater.flush();
    creater.close();


    FileStatus status = fs.getFileStatus(tablePath);
    long fileLen = status.getLen();
    FileFragment tablet = new FileFragment("table1_1", status.getPath(), 0, fileLen);

    tuple = new VTuple(keySchema.size());
    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "testBuildIndexWithAppender_" + storeType + ".idx"),
        keySchema, comp);
    reader.open();
    SeekableScanner scanner = TableSpaceManager.getSeekableScanner(conf, meta, schema, tablet, schema);
    scanner.init();

    for (int i = 0; i < TUPLE_NUM - 1; i++) {
      tuple.put(0, DatumFactory.createInt8(i));
      tuple.put(1, DatumFactory.createFloat8(i));
      long offsets = reader.find(tuple);
      scanner.seek(offsets);
      tuple = scanner.next();
      assertTrue("[seek check " + (i) + " ]", (i) == (tuple.getInt8(1)));
      assertTrue("[seek check " + (i) + " ]", (i) == (tuple.getFloat8(2)));

      offsets = reader.next();
      if (offsets == -1) {
        continue;
      }
      scanner.seek(offsets);
      tuple = scanner.next();
      assertTrue("[seek check " + (i + 1) + " ]", (i + 1) == (tuple.getInt4(0)));
      assertTrue("[seek check " + (i + 1) + " ]", (i + 1) == (tuple.getInt8(1)));
    }
    reader.close();
    scanner.close();
  }

  @Test
  public void testFindOmittedValue() throws IOException {
    meta = CatalogUtil.newTableMeta(storeType);

    Path tablePath = StorageUtil.concatPath(testDir, "testFindOmittedValue_" + storeType);
    Appender appender = ((FileTablespace) TableSpaceManager.getFileStorageManager(conf)).getAppender(meta, schema, tablePath);
    appender.init();
    Tuple tuple;
    for (int i = 0; i < TUPLE_NUM; i += 2) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt4(i));
      tuple.put(1, DatumFactory.createInt8(i));
      tuple.put(2, DatumFactory.createFloat8(i));
      tuple.put(3, DatumFactory.createFloat4(i));
      tuple.put(4, DatumFactory.createText("field_" + i));
      appender.addTuple(tuple);
    }
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    FileFragment tablet = new FileFragment("table1_1", status.getPath(), 0, status.getLen());

    SortSpec[] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumn("long"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumn("double"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("long", Type.INT8));
    keySchema.addColumn(new Column("double", Type.FLOAT8));

    BaseTupleComparator comp = new BaseTupleComparator(keySchema, sortKeys);

    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir, "testFindOmittedValue_" + storeType + ".idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner = TableSpaceManager.getSeekableScanner(conf, meta, schema, tablet, schema);
    scanner.init();

    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;

      keyTuple.put(0, tuple.asDatum(1));
      keyTuple.put(1, tuple.asDatum(2));
      creater.write(keyTuple, offset);
    }

    creater.flush();
    creater.close();
    scanner.close();

    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "testFindOmittedValue_" + storeType + ".idx"),
        keySchema, comp);
    reader.open();
    for (int i = 1; i < TUPLE_NUM - 1; i += 2) {
      keyTuple.put(0, DatumFactory.createInt8(i));
      keyTuple.put(1, DatumFactory.createFloat8(i));
      long offsets = reader.find(keyTuple);
      assertEquals(-1, offsets);
    }
    reader.close();
  }

  @Test
  public void testFindNextKeyValue() throws IOException {
    meta = CatalogUtil.newTableMeta(storeType);

    Path tablePath = new Path(testDir, "testFindNextKeyValue_" + storeType);
    Appender appender = ((FileTablespace) TableSpaceManager.getFileStorageManager(conf)).getAppender(meta, schema, tablePath);
    appender.init();
    Tuple tuple;
    for (int i = 0; i < TUPLE_NUM; i++) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt4(i));
      tuple.put(1, DatumFactory.createInt8(i));
      tuple.put(2, DatumFactory.createFloat8(i));
      tuple.put(3, DatumFactory.createFloat4(i));
      tuple.put(4, DatumFactory.createText("field_" + i));
      appender.addTuple(tuple);
    }
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    long fileLen = status.getLen();
    FileFragment tablet = new FileFragment("table1_1", status.getPath(), 0, fileLen);

    SortSpec[] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumn("int"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumn("long"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("int", Type.INT4));
    keySchema.addColumn(new Column("long", Type.INT8));

    BaseTupleComparator comp = new BaseTupleComparator(keySchema, sortKeys);

    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir, "testFindNextKeyValue_" + storeType + ".idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner = TableSpaceManager.getSeekableScanner(conf, meta, schema, tablet, schema);
    scanner.init();

    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;

      keyTuple.put(0, tuple.asDatum(0));
      keyTuple.put(1, tuple.asDatum(1));
      creater.write(keyTuple, offset);
    }

    creater.flush();
    creater.close();
    scanner.close();

    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "testFindNextKeyValue_" + storeType + ".idx"),
        keySchema, comp);
    reader.open();
    scanner = TableSpaceManager.getSeekableScanner(conf, meta, schema, tablet, schema);
    scanner.init();

    Tuple result;
    for (int i = 0; i < TUPLE_NUM - 1; i++) {
      keyTuple = new VTuple(2);
      keyTuple.put(0, DatumFactory.createInt4(i));
      keyTuple.put(1, DatumFactory.createInt8(i));
      long offsets = reader.find(keyTuple, true);
      scanner.seek(offsets);
      result = scanner.next();
      assertTrue("[seek check " + (i + 1) + " ]",
          (i + 1) == (result.getInt4(0)));
      assertTrue("[seek check " + (i + 1) + " ]", (i + 1) == (result.getInt8(1)));

      offsets = reader.next();
      if (offsets == -1) {
        continue;
      }
      scanner.seek(offsets);
      result = scanner.next();
      assertTrue("[seek check " + (i + 2) + " ]", (i + 2) == (result.getInt8(0)));
      assertTrue("[seek check " + (i + 2) + " ]", (i + 2) == (result.getFloat8(1)));
    }
    reader.close();
    scanner.close();
  }

  @Test
  public void testFindNextKeyOmittedValue() throws IOException {
    meta = CatalogUtil.newTableMeta(storeType);

    Path tablePath = new Path(testDir, "testFindNextKeyOmittedValue_" + storeType);
    Appender appender = ((FileTablespace) TableSpaceManager.getFileStorageManager(conf))
        .getAppender(meta, schema, tablePath);
    appender.init();
    Tuple tuple;
    for (int i = 0; i < TUPLE_NUM; i += 2) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt4(i));
      tuple.put(1, DatumFactory.createInt8(i));
      tuple.put(2, DatumFactory.createFloat8(i));
      tuple.put(3, DatumFactory.createFloat4(i));
      tuple.put(4, DatumFactory.createText("field_" + i));
      appender.addTuple(tuple);
    }
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    long fileLen = status.getLen();
    FileFragment tablet = new FileFragment("table1_1", status.getPath(), 0, fileLen);

    SortSpec[] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumn("int"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumn("long"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("int", Type.INT4));
    keySchema.addColumn(new Column("long", Type.INT8));

    BaseTupleComparator comp = new BaseTupleComparator(keySchema, sortKeys);

    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir, "testFindNextKeyOmittedValue_" + storeType + ".idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner = TableSpaceManager.getSeekableScanner(conf, meta, schema, tablet, schema);
    scanner.init();

    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;

      keyTuple.put(0, tuple.asDatum(0));
      keyTuple.put(1, tuple.asDatum(1));
      creater.write(keyTuple, offset);
    }

    creater.flush();
    creater.close();
    scanner.close();

    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "testFindNextKeyOmittedValue_" + storeType + ".idx"),
        keySchema, comp);
    reader.open();
    scanner = TableSpaceManager.getSeekableScanner(conf, meta, schema, tablet, schema);
    scanner.init();

    Tuple result;
    for (int i = 1; i < TUPLE_NUM - 1; i += 2) {
      keyTuple = new VTuple(2);
      keyTuple.put(0, DatumFactory.createInt4(i));
      keyTuple.put(1, DatumFactory.createInt8(i));
      long offsets = reader.find(keyTuple, true);
      scanner.seek(offsets);
      result = scanner.next();
      assertTrue("[seek check " + (i + 1) + " ]", (i + 1) == (result.getInt4(0)));
      assertTrue("[seek check " + (i + 1) + " ]", (i + 1) == (result.getInt8(1)));
    }
    scanner.close();
  }

  @Test
  public void testFindMinValue() throws IOException {
    meta = CatalogUtil.newTableMeta(storeType);

    Path tablePath = new Path(testDir, "testFindMinValue" + storeType);
    Appender appender = ((FileTablespace) TableSpaceManager.getFileStorageManager(conf)).getAppender(meta, schema, tablePath);
    appender.init();

    Tuple tuple;
    for (int i = 5; i < TUPLE_NUM + 5; i++) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt4(i));
      tuple.put(1, DatumFactory.createInt8(i));
      tuple.put(2, DatumFactory.createFloat8(i));
      tuple.put(3, DatumFactory.createFloat4(i));
      tuple.put(4, DatumFactory.createText("field_" + i));
      appender.addTuple(tuple);
    }
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    long fileLen = status.getLen();
    FileFragment tablet = new FileFragment("table1_1", status.getPath(), 0, fileLen);

    SortSpec[] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumn("long"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumn("double"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("long", Type.INT8));
    keySchema.addColumn(new Column("double", Type.FLOAT8));

    BaseTupleComparator comp = new BaseTupleComparator(keySchema, sortKeys);
    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir, "testFindMinValue_" + storeType + ".idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner = TableSpaceManager.getSeekableScanner(conf, meta, schema, tablet, schema);
    scanner.init();

    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;

      keyTuple.put(0, tuple.asDatum(1));
      keyTuple.put(1, tuple.asDatum(2));
      creater.write(keyTuple, offset);
    }

    creater.flush();
    creater.close();
    scanner.close();

    tuple = new VTuple(keySchema.size());

    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "testFindMinValue_" + storeType + ".idx"),
        keySchema, comp);
    reader.open();
    scanner = TableSpaceManager.getSeekableScanner(conf, meta, schema, tablet, schema);
    scanner.init();

    tuple.put(0, DatumFactory.createInt8(0));
    tuple.put(1, DatumFactory.createFloat8(0));

    offset = reader.find(tuple);
    assertEquals(-1, offset);

    offset = reader.find(tuple, true);
    assertTrue(offset >= 0);
    scanner.seek(offset);
    tuple = scanner.next();
    assertEquals(5, tuple.getInt4(1));
    assertEquals(5l, tuple.getInt8(2));
    reader.close();
    scanner.close();
  }

  @Test
  public void testMinMax() throws IOException {
    meta = CatalogUtil.newTableMeta(storeType);

    Path tablePath = new Path(testDir, "testMinMax_" + storeType);
    Appender appender = ((FileTablespace) TableSpaceManager.getFileStorageManager(conf)).getAppender(meta, schema, tablePath);
    appender.init();
    Tuple tuple;
    for (int i = 5; i < TUPLE_NUM; i += 2) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt4(i));
      tuple.put(1, DatumFactory.createInt8(i));
      tuple.put(2, DatumFactory.createFloat8(i));
      tuple.put(3, DatumFactory.createFloat4(i));
      tuple.put(4, DatumFactory.createText("field_" + i));
      appender.addTuple(tuple);
    }
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    long fileLen = status.getLen();
    FileFragment tablet = new FileFragment("table1_1", status.getPath(), 0, fileLen);

    SortSpec[] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumn("int"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumn("long"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("int", Type.INT4));
    keySchema.addColumn(new Column("long", Type.INT8));

    BaseTupleComparator comp = new BaseTupleComparator(keySchema, sortKeys);

    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir, "testMinMax_" + storeType + ".idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner = TableSpaceManager.getSeekableScanner(conf, meta, schema, tablet, schema);
    scanner.init();

    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;

      keyTuple.put(0, tuple.asDatum(0));
      keyTuple.put(1, tuple.asDatum(1));
      creater.write(keyTuple, offset);
    }

    creater.flush();
    creater.close();
    scanner.close();

    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "testMinMax_" + storeType + ".idx"),
        keySchema, comp);
    reader.open();

    Tuple min = reader.getFirstKey();
    assertEquals(5, min.getInt4(0));
    assertEquals(5l, min.getInt8(0));

    Tuple max = reader.getLastKey();
    assertEquals(TUPLE_NUM - 1, max.getInt4(0));
    assertEquals(TUPLE_NUM - 1, max.getInt8(0));
    reader.close();
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
        findKey.put(0, DatumFactory.createInt4(keyVal));
        findKey.put(1, DatumFactory.createInt8(keyVal));
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
    meta = CatalogUtil.newTableMeta(storeType);

    Path tablePath = new Path(testDir, "testConcurrentAccess_" + storeType);
    Appender appender = ((FileTablespace) TableSpaceManager.getFileStorageManager(conf)).getAppender(meta, schema, tablePath);
    appender.init();

    Tuple tuple;
    for (int i = 0; i < TUPLE_NUM; i++) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt4(i));
      tuple.put(1, DatumFactory.createInt8(i));
      tuple.put(2, DatumFactory.createFloat8(i));
      tuple.put(3, DatumFactory.createFloat4(i));
      tuple.put(4, DatumFactory.createText("field_" + i));
      appender.addTuple(tuple);
    }
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    long fileLen = status.getLen();
    FileFragment tablet = new FileFragment("table1_1", status.getPath(), 0, fileLen);

    SortSpec[] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumn("int"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumn("long"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("int", Type.INT4));
    keySchema.addColumn(new Column("long", Type.INT8));

    BaseTupleComparator comp = new BaseTupleComparator(keySchema, sortKeys);

    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir, "testConcurrentAccess_" + storeType + ".idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner = TableSpaceManager.getSeekableScanner(conf, meta, schema, tablet, schema);
    scanner.init();

    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;

      keyTuple.put(0, tuple.asDatum(0));
      keyTuple.put(1, tuple.asDatum(1));
      creater.write(keyTuple, offset);
    }

    creater.flush();
    creater.close();
    scanner.close();

    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "testConcurrentAccess_" + storeType + ".idx"),
        keySchema, comp);
    reader.open();

    Thread[] threads = new Thread[5];
    ConcurrentAccessor[] accs = new ConcurrentAccessor[5];
    for (int i = 0; i < threads.length; i++) {
      accs[i] = new ConcurrentAccessor(reader);
      threads[i] = new Thread(accs[i]);
      threads[i].start();
    }

    for (int i = 0; i < threads.length; i++) {
      threads[i].join();
      assertFalse(accs[i].isFailed());
    }
    reader.close();
  }


  @Test
  public void testFindValueDescOrder() throws IOException {
    meta = CatalogUtil.newTableMeta(storeType);

    Path tablePath = new Path(testDir, "testFindValueDescOrder_" + storeType);
    Appender appender = ((FileTablespace) TableSpaceManager.getFileStorageManager(conf)).getAppender(meta, schema, tablePath);
    appender.init();

    Tuple tuple;
    for (int i = (TUPLE_NUM - 1); i >= 0; i--) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt4(i));
      tuple.put(1, DatumFactory.createInt8(i));
      tuple.put(2, DatumFactory.createFloat8(i));
      tuple.put(3, DatumFactory.createFloat4(i));
      tuple.put(4, DatumFactory.createText("field_" + i));
      appender.addTuple(tuple);
    }
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    long fileLen = status.getLen();
    FileFragment tablet = new FileFragment("table1_1", status.getPath(), 0, fileLen);

    SortSpec[] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumn("long"), false, false);
    sortKeys[1] = new SortSpec(schema.getColumn("double"), false, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("long", Type.INT8));
    keySchema.addColumn(new Column("double", Type.FLOAT8));

    BaseTupleComparator comp = new BaseTupleComparator(keySchema, sortKeys);


    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir, "testFindValueDescOrder_" + storeType + ".idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner = TableSpaceManager.getSeekableScanner(conf, meta, schema, tablet, schema);
    scanner.init();

    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;

      keyTuple.put(0, tuple.asDatum(1));
      keyTuple.put(1, tuple.asDatum(2));
      creater.write(keyTuple, offset);
    }

    creater.flush();
    creater.close();
    scanner.close();

    tuple = new VTuple(keySchema.size());

    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "testFindValueDescOrder_" + storeType + ".idx"),
        keySchema, comp);
    reader.open();
    scanner = TableSpaceManager.getSeekableScanner(conf, meta, schema, tablet, schema);
    scanner.init();

    for (int i = (TUPLE_NUM - 1); i > 0; i--) {
      tuple.put(0, DatumFactory.createInt8(i));
      tuple.put(1, DatumFactory.createFloat8(i));
      long offsets = reader.find(tuple);
      scanner.seek(offsets);
      tuple = scanner.next();
      assertTrue("seek check [" + (i) + " ," + (tuple.getInt8(1)) + "]", (i) == (tuple.getInt8(1)));
      assertTrue("seek check [" + (i) + " ," + (tuple.getFloat8(2)) + "]", (i) == (tuple.getFloat8(2)));

      offsets = reader.next();
      if (offsets == -1) {
        continue;
      }
      scanner.seek(offsets);
      tuple = scanner.next();
      assertTrue("[seek check " + (i - 1) + " ]", (i - 1) == (tuple.getInt4(0)));
      assertTrue("[seek check " + (i - 1) + " ]", (i - 1) == (tuple.getInt8(1)));
    }
    reader.close();
    scanner.close();
  }

  @Test
  public void testFindNextKeyValueDescOrder() throws IOException {
    meta = CatalogUtil.newTableMeta(storeType);

    Path tablePath = new Path(testDir, "testFindNextKeyValueDescOrder_" + storeType);
    Appender appender = ((FileTablespace) TableSpaceManager.getFileStorageManager(conf)).getAppender(meta, schema, tablePath);
    appender.init();

    Tuple tuple;
    for (int i = (TUPLE_NUM - 1); i >= 0; i--) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt4(i));
      tuple.put(1, DatumFactory.createInt8(i));
      tuple.put(2, DatumFactory.createFloat8(i));
      tuple.put(3, DatumFactory.createFloat4(i));
      tuple.put(4, DatumFactory.createText("field_" + i));
      appender.addTuple(tuple);
    }
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    long fileLen = status.getLen();
    FileFragment tablet = new FileFragment("table1_1", status.getPath(), 0, fileLen);

    SortSpec[] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumn("int"), false, false);
    sortKeys[1] = new SortSpec(schema.getColumn("long"), false, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("int", Type.INT4));
    keySchema.addColumn(new Column("long", Type.INT8));

    BaseTupleComparator comp = new BaseTupleComparator(keySchema, sortKeys);

    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir,
        "testFindNextKeyValueDescOrder_" + storeType + ".idx"), BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner = TableSpaceManager.getSeekableScanner(conf, meta, schema, tablet, schema);
    scanner.init();

    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;

      keyTuple.put(0, tuple.asDatum(0));
      keyTuple.put(1, tuple.asDatum(1));
      creater.write(keyTuple, offset);
    }

    creater.flush();
    creater.close();
    scanner.close();


    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "testFindNextKeyValueDescOrder_" + storeType + ".idx"),
        keySchema, comp);
    reader.open();

    assertEquals(keySchema, reader.getKeySchema());
    assertEquals(comp, reader.getComparator());

    scanner = TableSpaceManager.getSeekableScanner(conf, meta, schema, tablet, schema);
    scanner.init();

    Tuple result;
    for (int i = (TUPLE_NUM - 1); i > 0; i--) {
      keyTuple = new VTuple(2);
      keyTuple.put(0, DatumFactory.createInt4(i));
      keyTuple.put(1, DatumFactory.createInt8(i));
      long offsets = reader.find(keyTuple, true);
      scanner.seek(offsets);
      result = scanner.next();
      assertTrue("[seek check " + (i - 1) + " ]",
          (i - 1) == (result.getInt4(0)));
      assertTrue("[seek check " + (i - 1) + " ]", (i - 1) == (result.getInt8(1)));

      offsets = reader.next();
      if (offsets == -1) {
        continue;
      }
      scanner.seek(offsets);
      result = scanner.next();
      assertTrue("[seek check " + (i - 2) + " ]", (i - 2) == (result.getInt8(0)));
      assertTrue("[seek check " + (i - 2) + " ]", (i - 2) == (result.getFloat8(1)));
    }
    reader.close();
    scanner.close();
  }
}
