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
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.index.bst.BSTIndex;
import org.apache.tajo.storage.index.bst.BSTIndex.BSTIndexReader;
import org.apache.tajo.storage.index.bst.BSTIndex.BSTIndexWriter;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tajo.storage.CSVFile.CSVScanner;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSingleCSVFileBSTIndex {
  
  private TajoConf conf;
  private Schema schema;
  private TableMeta meta;
  private FileSystem fs;

  private static final int TUPLE_NUM = 10000;
  private static final int LOAD_NUM = 100;
  private static final String TEST_PATH = "target/test-data/TestSingleCSVFileBSTIndex";
  private Path testDir;
  
  public TestSingleCSVFileBSTIndex() {
    conf = new TajoConf();
    conf.setVar(ConfVars.ROOT_DIR, TEST_PATH);
    schema = new Schema();
    schema.addColumn(new Column("int", Type.INT4));
    schema.addColumn(new Column("long", Type.INT8));
    schema.addColumn(new Column("double", Type.FLOAT8));
    schema.addColumn(new Column("float", Type.FLOAT4));
    schema.addColumn(new Column("string", Type.TEXT));
  }

  @Before
  public void setUp() throws Exception {
    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    fs = testDir.getFileSystem(conf);
  }

  @Test
  public void testFindValueInSingleCSV() throws IOException {
    meta = CatalogUtil.newTableMeta("CSV");

    Path tablePath = StorageUtil.concatPath(testDir, "testFindValueInSingleCSV", "table.csv");
    fs.mkdirs(tablePath.getParent());

    Appender appender = ((FileTablespace) TablespaceManager.getLocalFs()).getAppender(meta, schema, tablePath);
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
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir,
        "FindValueInCSV.idx"), BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner fileScanner = new CSVScanner(conf, schema, meta, tablet);
    fileScanner.init();
    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = fileScanner.getNextOffset();
      tuple = fileScanner.next();
      if (tuple == null)
        break;

      keyTuple.put(0, tuple.asDatum(1));
      keyTuple.put(1, tuple.asDatum(2));
      creater.write(keyTuple, offset);
    }

    creater.flush();
    creater.close();
    fileScanner.close();

    tuple = new VTuple(keySchema.size());
    BSTIndexReader reader = bst.getIndexReader(new Path(testDir,
        "FindValueInCSV.idx"), keySchema, comp);
    reader.open();
    fileScanner = new CSVScanner(conf, schema, meta, tablet);
    fileScanner.init();
    for (int i = 0; i < TUPLE_NUM - 1; i++) {
      tuple.put(0, DatumFactory.createInt8(i));
      tuple.put(1, DatumFactory.createFloat8(i));
      long offsets = reader.find(tuple);
      fileScanner.seek(offsets);
      tuple = fileScanner.next();
      assertEquals(i,  (tuple.getInt8(1)));
      assertEquals(i, (tuple.getFloat8(2)) , 0.01);

      offsets = reader.next();
      if (offsets == -1) {
        continue;
      }
      fileScanner.seek(offsets);
      tuple = fileScanner.next();
      assertTrue("[seek check " + (i + 1) + " ]",
          (i + 1) == (tuple.getInt4(0)));
      assertTrue("[seek check " + (i + 1) + " ]",
          (i + 1) == (tuple.getInt8(1)));
    }
  }

  @Test
  public void testFindNextKeyValueInSingleCSV() throws IOException {
    meta = CatalogUtil.newTableMeta("CSV");

    Path tablePath = StorageUtil.concatPath(testDir, "testFindNextKeyValueInSingleCSV",
        "table1.csv");
    fs.mkdirs(tablePath.getParent());
    Appender appender = ((FileTablespace) TablespaceManager.getLocalFs()).getAppender(meta, schema, tablePath);
    appender.init();
    Tuple tuple;
    for(int i = 0 ; i < TUPLE_NUM; i ++ ) {
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
    
    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumn("int"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumn("long"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("int", Type.INT4));
    keySchema.addColumn(new Column("long", Type.INT8));

    BaseTupleComparator comp = new BaseTupleComparator(keySchema, sortKeys);
    
    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir, "FindNextKeyValueInCSV.idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();
    
    SeekableScanner fileScanner  = new CSVScanner(conf, schema, meta, tablet);
    fileScanner.init();
    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = fileScanner.getNextOffset();
      tuple = fileScanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.asDatum(0));
      keyTuple.put(1, tuple.asDatum(1));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    fileScanner.close();    
    
    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "FindNextKeyValueInCSV.idx"), keySchema, comp);
    reader.open();
    fileScanner  = new CSVScanner(conf, schema, meta, tablet);
    fileScanner.init();
    Tuple result;
    for(int i = 0 ; i < TUPLE_NUM -1 ; i ++) {
      keyTuple = new VTuple(2);
      keyTuple.put(0, DatumFactory.createInt4(i));
      keyTuple.put(1, DatumFactory.createInt8(i));
      long offsets = reader.find(keyTuple, true);
      fileScanner.seek(offsets);
      result = fileScanner.next();
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (result.getInt4(0)));
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (result.getInt8(1)));
      
      offsets = reader.next();
      if (offsets == -1) {
        continue;
      }
      fileScanner.seek(offsets);
      result = fileScanner.next();
      assertTrue("[seek check " + (i + 2) + " ]" , (i + 2) == (result.getInt8(0)));
      assertTrue("[seek check " + (i + 2) + " ]" , (i + 2) == (result.getFloat8(1)));
    }
  }
}
