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

package org.apache.tajo.storage;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.QueryId;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.ProtobufDatumFactory;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.rcfile.RCFile;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestStorages {
	private TajoConf conf;
	private static String TEST_PATH = "target/test-data/TestStorages";

  private StoreType storeType;
  private boolean splitable;
  private boolean statsable;
  private Path testDir;
  private FileSystem fs;

  public TestStorages(StoreType type, boolean splitable, boolean statsable) throws IOException {
    this.storeType = type;
    this.splitable = splitable;
    this.statsable = statsable;

    conf = new TajoConf();

    if (storeType == StoreType.RCFILE) {
      conf.setInt(RCFile.RECORD_INTERVAL_CONF_STR, 100);
    }


    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    fs = testDir.getFileSystem(conf);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> generateParameters() {
    return Arrays.asList(new Object[][] {
        {StoreType.CSV, true, true},
        {StoreType.RAW, false, false},
        {StoreType.RCFILE, true, true},
        {StoreType.PARQUET, false, false},
        {StoreType.SEQUENCEFILE, true, true},
    });
  }

	@Test
  public void testSplitable() throws IOException {
    if (splitable) {
      Schema schema = new Schema();
      schema.addColumn("id", Type.INT4);
      schema.addColumn("age", Type.INT8);

      TableMeta meta = CatalogUtil.newTableMeta(storeType);
      Path tablePath = new Path(testDir, "Splitable.data");
      Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, schema, tablePath);
      appender.enableStats();
      appender.init();
      int tupleNum = 10000;
      VTuple vTuple;

      for(int i = 0; i < tupleNum; i++) {
        vTuple = new VTuple(2);
        vTuple.put(0, DatumFactory.createInt4(i + 1));
        vTuple.put(1, DatumFactory.createInt8(25l));
        appender.addTuple(vTuple);
      }
      appender.close();
      TableStats stat = appender.getStats();
      assertEquals(tupleNum, stat.getNumRows().longValue());

      FileStatus status = fs.getFileStatus(tablePath);
      long fileLen = status.getLen();
      long randomNum = (long) (Math.random() * fileLen) + 1;

      FileFragment[] tablets = new FileFragment[2];
      tablets[0] = new FileFragment("Splitable", tablePath, 0, randomNum);
      tablets[1] = new FileFragment("Splitable", tablePath, randomNum, (fileLen - randomNum));

      Scanner scanner = StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, tablets[0], schema);
      assertTrue(scanner.isSplittable());
      scanner.init();
      int tupleCnt = 0;
      while (scanner.next() != null) {
        tupleCnt++;
      }
      scanner.close();

      scanner = StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, tablets[1], schema);
      assertTrue(scanner.isSplittable());
      scanner.init();
      while (scanner.next() != null) {
        tupleCnt++;
      }
      scanner.close();

      assertEquals(tupleNum, tupleCnt);
    }
	}

  @Test
  public void testProjection() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("age", Type.INT8);
    schema.addColumn("score", Type.FLOAT4);

    TableMeta meta = CatalogUtil.newTableMeta(storeType);
    meta.setOptions(CatalogUtil.newOptionsWithDefault(storeType));

    Path tablePath = new Path(testDir, "testProjection.data");
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, schema, tablePath);
    appender.init();
    int tupleNum = 10000;
    VTuple vTuple;

    for(int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(3);
      vTuple.put(0, DatumFactory.createInt4(i + 1));
      vTuple.put(1, DatumFactory.createInt8(i + 2));
      vTuple.put(2, DatumFactory.createFloat4(i + 3));
      appender.addTuple(vTuple);
    }
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    FileFragment fragment = new FileFragment("testReadAndWrite", tablePath, 0, status.getLen());

    Schema target = new Schema();
    target.addColumn("age", Type.INT8);
    target.addColumn("score", Type.FLOAT4);
    Scanner scanner = StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, fragment, target);
    scanner.init();
    int tupleCnt = 0;
    Tuple tuple;
    while ((tuple = scanner.next()) != null) {
      if (storeType == StoreType.RCFILE
          || storeType == StoreType.TREVNI
          || storeType == StoreType.CSV
          || storeType == StoreType.PARQUET
          || storeType == StoreType.SEQUENCEFILE) {
        assertTrue(tuple.get(0) == null);
      }
      assertTrue(tupleCnt + 2 == tuple.get(1).asInt8());
      assertTrue(tupleCnt + 3 == tuple.get(2).asFloat4());
      tupleCnt++;
    }
    scanner.close();

    assertEquals(tupleNum, tupleCnt);
  }

  @Test
  public void testVariousTypes() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("col1", Type.BOOLEAN);
    schema.addColumn("col2", Type.BIT);
    schema.addColumn("col3", Type.CHAR, 7);
    schema.addColumn("col4", Type.INT2);
    schema.addColumn("col5", Type.INT4);
    schema.addColumn("col6", Type.INT8);
    schema.addColumn("col7", Type.FLOAT4);
    schema.addColumn("col8", Type.FLOAT8);
    schema.addColumn("col9", Type.TEXT);
    schema.addColumn("col10", Type.BLOB);
    schema.addColumn("col11", Type.INET4);
    schema.addColumn("col12", Type.NULL_TYPE);
    schema.addColumn("col13", CatalogUtil.newDataType(Type.PROTOBUF, TajoIdProtos.QueryIdProto.class.getName()));

    Options options = new Options();
    TableMeta meta = CatalogUtil.newTableMeta(storeType, options);
    meta.setOptions(CatalogUtil.newOptionsWithDefault(storeType));

    Path tablePath = new Path(testDir, "testVariousTypes.data");
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, schema, tablePath);
    appender.init();

    QueryId queryid = new QueryId("12345", 5);
    ProtobufDatumFactory factory = ProtobufDatumFactory.get(TajoIdProtos.QueryIdProto.class.getName());

    Tuple tuple = new VTuple(13);
    tuple.put(new Datum[] {
        DatumFactory.createBool(true),
        DatumFactory.createBit((byte) 0x99),
        DatumFactory.createChar("hyunsik"),
        DatumFactory.createInt2((short) 17),
        DatumFactory.createInt4(59),
        DatumFactory.createInt8(23l),
        DatumFactory.createFloat4(77.9f),
        DatumFactory.createFloat8(271.9f),
        DatumFactory.createText("hyunsik"),
        DatumFactory.createBlob("hyunsik".getBytes()),
        DatumFactory.createInet4("192.168.0.1"),
        NullDatum.get(),
        factory.createDatum(queryid.getProto())
    });
    appender.addTuple(tuple);
    appender.flush();
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner =  StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, fragment);
    scanner.init();

    Tuple retrieved;
    while ((retrieved=scanner.next()) != null) {
      for (int i = 0; i < tuple.size(); i++) {
        assertEquals(tuple.get(i), retrieved.get(i));
      }
    }
    scanner.close();
  }

  @Test
  public void testNullHandlingTypes() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("col1", Type.BOOLEAN);
    schema.addColumn("col2", Type.BIT);
    schema.addColumn("col3", Type.CHAR, 7);
    schema.addColumn("col4", Type.INT2);
    schema.addColumn("col5", Type.INT4);
    schema.addColumn("col6", Type.INT8);
    schema.addColumn("col7", Type.FLOAT4);
    schema.addColumn("col8", Type.FLOAT8);
    schema.addColumn("col9", Type.TEXT);
    schema.addColumn("col10", Type.BLOB);
    schema.addColumn("col11", Type.INET4);
    schema.addColumn("col12", Type.NULL_TYPE);
    schema.addColumn("col13", CatalogUtil.newDataType(Type.PROTOBUF, TajoIdProtos.QueryIdProto.class.getName()));

    Options options = new Options();
    TableMeta meta = CatalogUtil.newTableMeta(storeType, options);
    meta.setOptions(CatalogUtil.newOptionsWithDefault(storeType));
    meta.putOption(CatalogConstants.CSVFILE_NULL, "\\\\N");
    meta.putOption(CatalogConstants.RCFILE_NULL, "\\\\N");
    meta.putOption(CatalogConstants.RCFILE_SERDE, TextSerializerDeserializer.class.getName());
    meta.putOption(CatalogConstants.SEQUENCEFILE_NULL, "\\");

    Path tablePath = new Path(testDir, "testVariousTypes.data");
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, schema, tablePath);
    appender.init();

    QueryId queryid = new QueryId("12345", 5);
    ProtobufDatumFactory factory = ProtobufDatumFactory.get(TajoIdProtos.QueryIdProto.class.getName());

    Tuple seedTuple = new VTuple(13);
    seedTuple.put(new Datum[]{
        DatumFactory.createBool(true),                // 0
        DatumFactory.createBit((byte) 0x99),          // 1
        DatumFactory.createChar("hyunsik"),           // 2
        DatumFactory.createInt2((short) 17),          // 3
        DatumFactory.createInt4(59),                  // 4
        DatumFactory.createInt8(23l),                 // 5
        DatumFactory.createFloat4(77.9f),             // 6
        DatumFactory.createFloat8(271.9f),            // 7
        DatumFactory.createText("hyunsik"),           // 8
        DatumFactory.createBlob("hyunsik".getBytes()),// 9
        DatumFactory.createInet4("192.168.0.1"),      // 10
        NullDatum.get(),                              // 11
        factory.createDatum(queryid.getProto())       // 12
    });

    // Making tuples with different null column positions
    Tuple tuple;
    for (int i = 0; i < 13; i++) {
      tuple = new VTuple(13);
      for (int j = 0; j < 13; j++) {
        if (i == j) { // i'th column will have NULL value
          tuple.put(j, NullDatum.get());
        } else {
          tuple.put(j, seedTuple.get(j));
        }
      }
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner = StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, fragment);
    scanner.init();

    Tuple retrieved;
    int i = 0;
    while ((retrieved = scanner.next()) != null) {
      assertEquals(13, retrieved.size());
      for (int j = 0; j < 13; j++) {
        if (i == j) {
          assertEquals(NullDatum.get(), retrieved.get(j));
        } else {
          assertEquals(seedTuple.get(j), retrieved.get(j));
        }
      }

      i++;
    }
    scanner.close();
  }

  @Test
  public void testRCFileTextSerializeDeserialize() throws IOException {
    if(storeType != StoreType.RCFILE) return;

    Schema schema = new Schema();
    schema.addColumn("col1", Type.BOOLEAN);
    schema.addColumn("col2", Type.BIT);
    schema.addColumn("col3", Type.CHAR, 7);
    schema.addColumn("col4", Type.INT2);
    schema.addColumn("col5", Type.INT4);
    schema.addColumn("col6", Type.INT8);
    schema.addColumn("col7", Type.FLOAT4);
    schema.addColumn("col8", Type.FLOAT8);
    schema.addColumn("col9", Type.TEXT);
    schema.addColumn("col10", Type.BLOB);
    schema.addColumn("col11", Type.INET4);
    schema.addColumn("col12", Type.NULL_TYPE);
    schema.addColumn("col13", CatalogUtil.newDataType(Type.PROTOBUF, TajoIdProtos.QueryIdProto.class.getName()));

    Options options = new Options();
    TableMeta meta = CatalogUtil.newTableMeta(storeType, options);
    meta.putOption(CatalogConstants.CSVFILE_SERDE, TextSerializerDeserializer.class.getName());

    Path tablePath = new Path(testDir, "testVariousTypes.data");
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, schema, tablePath);
    appender.enableStats();
    appender.init();

    QueryId queryid = new QueryId("12345", 5);
    ProtobufDatumFactory factory = ProtobufDatumFactory.get(TajoIdProtos.QueryIdProto.class.getName());

    Tuple tuple = new VTuple(13);
    tuple.put(new Datum[] {
        DatumFactory.createBool(true),
        DatumFactory.createBit((byte) 0x99),
        DatumFactory.createChar("jinho"),
        DatumFactory.createInt2((short) 17),
        DatumFactory.createInt4(59),
        DatumFactory.createInt8(23l),
        DatumFactory.createFloat4(77.9f),
        DatumFactory.createFloat8(271.9f),
        DatumFactory.createText("jinho"),
        DatumFactory.createBlob("hyunsik babo".getBytes()),
        DatumFactory.createInet4("192.168.0.1"),
        NullDatum.get(),
        factory.createDatum(queryid.getProto())
    });
    appender.addTuple(tuple);
    appender.flush();
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    assertEquals(appender.getStats().getNumBytes().longValue(), status.getLen());

    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner =  StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, fragment);
    scanner.init();

    Tuple retrieved;
    while ((retrieved=scanner.next()) != null) {
      for (int i = 0; i < tuple.size(); i++) {
        assertEquals(tuple.get(i), retrieved.get(i));
      }
    }
    scanner.close();
    assertEquals(appender.getStats().getNumBytes().longValue(), scanner.getInputStats().getNumBytes().longValue());
    assertEquals(appender.getStats().getNumRows().longValue(), scanner.getInputStats().getNumRows().longValue());
  }

  @Test
  public void testRCFileBinarySerializeDeserialize() throws IOException {
    if(storeType != StoreType.RCFILE) return;

    Schema schema = new Schema();
    schema.addColumn("col1", Type.BOOLEAN);
    schema.addColumn("col2", Type.BIT);
    schema.addColumn("col3", Type.CHAR, 7);
    schema.addColumn("col4", Type.INT2);
    schema.addColumn("col5", Type.INT4);
    schema.addColumn("col6", Type.INT8);
    schema.addColumn("col7", Type.FLOAT4);
    schema.addColumn("col8", Type.FLOAT8);
    schema.addColumn("col9", Type.TEXT);
    schema.addColumn("col10", Type.BLOB);
    schema.addColumn("col11", Type.INET4);
    schema.addColumn("col12", Type.NULL_TYPE);
    schema.addColumn("col13", CatalogUtil.newDataType(Type.PROTOBUF, TajoIdProtos.QueryIdProto.class.getName()));

    Options options = new Options();
    TableMeta meta = CatalogUtil.newTableMeta(storeType, options);
    meta.putOption(CatalogConstants.RCFILE_SERDE, BinarySerializerDeserializer.class.getName());

    Path tablePath = new Path(testDir, "testVariousTypes.data");
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, schema, tablePath);
    appender.enableStats();
    appender.init();

    QueryId queryid = new QueryId("12345", 5);
    ProtobufDatumFactory factory = ProtobufDatumFactory.get(TajoIdProtos.QueryIdProto.class.getName());

    Tuple tuple = new VTuple(13);
    tuple.put(new Datum[] {
        DatumFactory.createBool(true),
        DatumFactory.createBit((byte) 0x99),
        DatumFactory.createChar("jinho"),
        DatumFactory.createInt2((short) 17),
        DatumFactory.createInt4(59),
        DatumFactory.createInt8(23l),
        DatumFactory.createFloat4(77.9f),
        DatumFactory.createFloat8(271.9f),
        DatumFactory.createText("jinho"),
        DatumFactory.createBlob("hyunsik babo".getBytes()),
        DatumFactory.createInet4("192.168.0.1"),
        NullDatum.get(),
        factory.createDatum(queryid.getProto())
    });
    appender.addTuple(tuple);
    appender.flush();
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    assertEquals(appender.getStats().getNumBytes().longValue(), status.getLen());

    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner =  StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, fragment);
    scanner.init();

    Tuple retrieved;
    while ((retrieved=scanner.next()) != null) {
      for (int i = 0; i < tuple.size(); i++) {
        assertEquals(tuple.get(i), retrieved.get(i));
      }
    }
    scanner.close();
    assertEquals(appender.getStats().getNumBytes().longValue(), scanner.getInputStats().getNumBytes().longValue());
    assertEquals(appender.getStats().getNumRows().longValue(), scanner.getInputStats().getNumRows().longValue());
  }

  @Test
  public void testSequenceFileTextSerializeDeserialize() throws IOException {
    if(storeType != StoreType.SEQUENCEFILE) return;

    Schema schema = new Schema();
    schema.addColumn("col1", Type.BOOLEAN);
    schema.addColumn("col2", Type.BIT);
    schema.addColumn("col3", Type.CHAR, 7);
    schema.addColumn("col4", Type.INT2);
    schema.addColumn("col5", Type.INT4);
    schema.addColumn("col6", Type.INT8);
    schema.addColumn("col7", Type.FLOAT4);
    schema.addColumn("col8", Type.FLOAT8);
    schema.addColumn("col9", Type.TEXT);
    schema.addColumn("col10", Type.BLOB);
    schema.addColumn("col11", Type.INET4);
    schema.addColumn("col12", Type.NULL_TYPE);
    schema.addColumn("col13", CatalogUtil.newDataType(Type.PROTOBUF, TajoIdProtos.QueryIdProto.class.getName()));

    Options options = new Options();
    TableMeta meta = CatalogUtil.newTableMeta(storeType, options);
    meta.putOption(CatalogConstants.SEQUENCEFILE_SERDE, TextSerializerDeserializer.class.getName());

    Path tablePath = new Path(testDir, "testVariousTypes.data");
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, schema, tablePath);
    appender.enableStats();
    appender.init();

    QueryId queryid = new QueryId("12345", 5);
    ProtobufDatumFactory factory = ProtobufDatumFactory.get(TajoIdProtos.QueryIdProto.class.getName());

    Tuple tuple = new VTuple(13);
    tuple.put(new Datum[] {
        DatumFactory.createBool(true),
        DatumFactory.createBit((byte) 0x99),
        DatumFactory.createChar("jinho"),
        DatumFactory.createInt2((short) 17),
        DatumFactory.createInt4(59),
        DatumFactory.createInt8(23l),
        DatumFactory.createFloat4(77.9f),
        DatumFactory.createFloat8(271.9f),
        DatumFactory.createText("jinho"),
        DatumFactory.createBlob("hyunsik babo".getBytes()),
        DatumFactory.createInet4("192.168.0.1"),
        NullDatum.get(),
        factory.createDatum(queryid.getProto())
    });
    appender.addTuple(tuple);
    appender.flush();
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    assertEquals(appender.getStats().getNumBytes().longValue(), status.getLen());

    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner =  StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, fragment);
    scanner.init();

    Tuple retrieved;
    while ((retrieved=scanner.next()) != null) {
      for (int i = 0; i < tuple.size(); i++) {
        assertEquals(tuple.get(i), retrieved.get(i));
      }
    }
    scanner.close();
    assertEquals(appender.getStats().getNumBytes().longValue(), scanner.getInputStats().getNumBytes().longValue());
    assertEquals(appender.getStats().getNumRows().longValue(), scanner.getInputStats().getNumRows().longValue());
  }

  @Test
  public void testSequenceFileBinarySerializeDeserialize() throws IOException {
    if(storeType != StoreType.SEQUENCEFILE) return;

    Schema schema = new Schema();
    schema.addColumn("col1", Type.BOOLEAN);
    schema.addColumn("col2", Type.BIT);
    schema.addColumn("col3", Type.CHAR, 7);
    schema.addColumn("col4", Type.INT2);
    schema.addColumn("col5", Type.INT4);
    schema.addColumn("col6", Type.INT8);
    schema.addColumn("col7", Type.FLOAT4);
    schema.addColumn("col8", Type.FLOAT8);
    schema.addColumn("col9", Type.TEXT);
    schema.addColumn("col10", Type.BLOB);
    schema.addColumn("col11", Type.INET4);
    schema.addColumn("col12", Type.NULL_TYPE);
    schema.addColumn("col13", CatalogUtil.newDataType(Type.PROTOBUF, TajoIdProtos.QueryIdProto.class.getName()));

    Options options = new Options();
    TableMeta meta = CatalogUtil.newTableMeta(storeType, options);
    meta.putOption(CatalogConstants.SEQUENCEFILE_SERDE, BinarySerializerDeserializer.class.getName());

    Path tablePath = new Path(testDir, "testVariousTypes.data");
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, schema, tablePath);
    appender.enableStats();
    appender.init();

    QueryId queryid = new QueryId("12345", 5);
    ProtobufDatumFactory factory = ProtobufDatumFactory.get(TajoIdProtos.QueryIdProto.class.getName());

    Tuple tuple = new VTuple(13);
    tuple.put(new Datum[] {
        DatumFactory.createBool(true),
        DatumFactory.createBit((byte) 0x99),
        DatumFactory.createChar("jinho"),
        DatumFactory.createInt2((short) 17),
        DatumFactory.createInt4(59),
        DatumFactory.createInt8(23l),
        DatumFactory.createFloat4(77.9f),
        DatumFactory.createFloat8(271.9f),
        DatumFactory.createText("jinho"),
        DatumFactory.createBlob("hyunsik babo".getBytes()),
        DatumFactory.createInet4("192.168.0.1"),
        NullDatum.get(),
        factory.createDatum(queryid.getProto())
    });
    appender.addTuple(tuple);
    appender.flush();
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    assertEquals(appender.getStats().getNumBytes().longValue(), status.getLen());

    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner =  StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, fragment);
    scanner.init();

    Tuple retrieved;
    while ((retrieved=scanner.next()) != null) {
      for (int i = 0; i < tuple.size(); i++) {
        assertEquals(tuple.get(i), retrieved.get(i));
      }
    }
    scanner.close();
    assertEquals(appender.getStats().getNumBytes().longValue(), scanner.getInputStats().getNumBytes().longValue());
    assertEquals(appender.getStats().getNumRows().longValue(), scanner.getInputStats().getNumRows().longValue());
  }

  @Test
  public void testTime() throws IOException {
    if (storeType == StoreType.CSV || storeType == StoreType.RAW) {
      Schema schema = new Schema();
      schema.addColumn("col1", Type.DATE);
      schema.addColumn("col2", Type.TIME);
      schema.addColumn("col3", Type.TIMESTAMP);

      Options options = new Options();
      TableMeta meta = CatalogUtil.newTableMeta(storeType, options);

      Path tablePath = new Path(testDir, "testTime.data");
      Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, schema, tablePath);
      appender.init();

      Tuple tuple = new VTuple(3);
      tuple.put(new Datum[]{
          DatumFactory.createDate("1980-04-01"),
          DatumFactory.createTime("12:34:56"),
          DatumFactory.createTimeStamp((int) System.currentTimeMillis() / 1000)
      });
      appender.addTuple(tuple);
      appender.flush();
      appender.close();

      FileStatus status = fs.getFileStatus(tablePath);
      FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
      Scanner scanner = StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, fragment);
      scanner.init();

      Tuple retrieved;
      while ((retrieved = scanner.next()) != null) {
        for (int i = 0; i < tuple.size(); i++) {
          assertEquals(tuple.get(i), retrieved.get(i));
        }
      }
      scanner.close();
    }
  }



}
