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

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.tajo.QueryId;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.ProtobufDatumFactory;
import org.apache.tajo.exception.ValueTooLongForTypeCharactersException;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.rcfile.RCFile;
import org.apache.tajo.storage.sequencefile.SequenceFileScanner;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.KeyValueSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestStorages {
	private TajoConf conf;
	private static String TEST_PATH = "target/test-data/TestStorages";

  private static String TEST_PROJECTION_AVRO_SCHEMA =
      "{\n" +
      "  \"type\": \"record\",\n" +
      "  \"namespace\": \"org.apache.tajo\",\n" +
      "  \"name\": \"testProjection\",\n" +
      "  \"fields\": [\n" +
      "    { \"name\": \"id\", \"type\": \"int\" },\n" +
      "    { \"name\": \"age\", \"type\": \"long\" },\n" +
      "    { \"name\": \"score\", \"type\": \"float\" }\n" +
      "  ]\n" +
      "}\n";

  private static String TEST_NULL_HANDLING_TYPES_AVRO_SCHEMA =
      "{\n" +
      "  \"type\": \"record\",\n" +
      "  \"namespace\": \"org.apache.tajo\",\n" +
      "  \"name\": \"testNullHandlingTypes\",\n" +
      "  \"fields\": [\n" +
      "    { \"name\": \"col1\", \"type\": [\"null\", \"boolean\"] },\n" +
      "    { \"name\": \"col2\", \"type\": [\"null\", \"string\"] },\n" +
      "    { \"name\": \"col3\", \"type\": [\"null\", \"int\"] },\n" +
      "    { \"name\": \"col4\", \"type\": [\"null\", \"int\"] },\n" +
      "    { \"name\": \"col5\", \"type\": [\"null\", \"long\"] },\n" +
      "    { \"name\": \"col6\", \"type\": [\"null\", \"float\"] },\n" +
      "    { \"name\": \"col7\", \"type\": [\"null\", \"double\"] },\n" +
      "    { \"name\": \"col8\", \"type\": [\"null\", \"string\"] },\n" +
      "    { \"name\": \"col9\", \"type\": [\"null\", \"bytes\"] },\n" +
      "    { \"name\": \"col10\", \"type\": [\"null\", \"bytes\"] },\n" +
      "    { \"name\": \"col11\", \"type\": \"null\" },\n" +
      "    { \"name\": \"col12\", \"type\": [\"null\", \"bytes\"] }\n" +
      "  ]\n" +
      "}\n";

  private static String TEST_MAX_VALUE_AVRO_SCHEMA =
      "{\n" +
          "  \"type\": \"record\",\n" +
          "  \"namespace\": \"org.apache.tajo\",\n" +
          "  \"name\": \"testMaxValue\",\n" +
          "  \"fields\": [\n" +
          "    { \"name\": \"col4\", \"type\": \"float\" },\n" +
          "    { \"name\": \"col5\", \"type\": \"double\" },\n" +
          "    { \"name\": \"col1\", \"type\": \"int\" },\n" +
          "    { \"name\": \"col2\", \"type\": \"int\" },\n" +
          "    { \"name\": \"col3\", \"type\": \"long\" }\n" +
          "  ]\n" +
          "}\n";

  private String storeType;
  private boolean splitable;
  private boolean statsable;
  private boolean seekable;
  private Path testDir;
  private FileSystem fs;

  public TestStorages(String type, boolean splitable, boolean statsable, boolean seekable) throws IOException {
    this.storeType = type;
    this.splitable = splitable;
    this.statsable = statsable;
    this.seekable = seekable;

    conf = new TajoConf();

    if (storeType.equalsIgnoreCase("RCFILE")) {
      conf.setInt(RCFile.RECORD_INTERVAL_CONF_STR, 100);
    }

    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    fs = testDir.getFileSystem(conf);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> generateParameters() {
    return Arrays.asList(new Object[][] {
        //type, splitable, statsable, seekable
        {"CSV", true, true, true},
        {"RAW", false, true, true},
        {"RCFILE", true, true, false},
        {"PARQUET", false, false, false},
        {"SEQUENCEFILE", true, true, false},
        {"AVRO", false, false, false},
        {"TEXT", true, true, true},
        {"JSON", true, true, false},
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
      FileTablespace sm = TablespaceManager.getLocalFs();
      Appender appender = sm.getAppender(meta, schema, tablePath);
      appender.enableStats();
      appender.init();
      int tupleNum = 10000;
      VTuple vTuple;

      for (int i = 0; i < tupleNum; i++) {
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

      Scanner scanner = sm.getScanner(meta, schema, tablets[0], schema);
      assertTrue(scanner.isSplittable());
      scanner.init();
      int tupleCnt = 0;
      while (scanner.next() != null) {
        tupleCnt++;
      }
      scanner.close();

      scanner = sm.getScanner(meta, schema, tablets[1], schema);
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
  public void testRCFileSplitable() throws IOException {
    if (storeType.equalsIgnoreCase("StoreType.RCFILE")) {
      Schema schema = new Schema();
      schema.addColumn("id", Type.INT4);
      schema.addColumn("age", Type.INT8);

      TableMeta meta = CatalogUtil.newTableMeta(storeType);
      Path tablePath = new Path(testDir, "Splitable.data");
      FileTablespace sm = TablespaceManager.getLocalFs();
      Appender appender = sm.getAppender(meta, schema, tablePath);
      appender.enableStats();
      appender.init();
      int tupleNum = 10000;
      VTuple vTuple;

      for (int i = 0; i < tupleNum; i++) {
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
      long randomNum = 122; // header size

      FileFragment[] tablets = new FileFragment[2];
      tablets[0] = new FileFragment("Splitable", tablePath, 0, randomNum);
      tablets[1] = new FileFragment("Splitable", tablePath, randomNum, (fileLen - randomNum));

      Scanner scanner = sm.getScanner(meta, schema, tablets[0], schema);
      assertTrue(scanner.isSplittable());
      scanner.init();
      int tupleCnt = 0;
      while (scanner.next() != null) {
        tupleCnt++;
      }
      scanner.close();

      scanner = sm.getScanner(meta, schema, tablets[1], schema);
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
    meta.setOptions(CatalogUtil.newDefaultProperty(storeType));
    if (storeType.equalsIgnoreCase("AVRO")) {
      meta.putOption(StorageConstants.AVRO_SCHEMA_LITERAL,
                     TEST_PROJECTION_AVRO_SCHEMA);
    }

    Path tablePath = new Path(testDir, "testProjection.data");
    FileTablespace sm = TablespaceManager.getLocalFs();
    Appender appender = sm.getAppender(meta, schema, tablePath);
    appender.init();
    int tupleNum = 10000;
    VTuple vTuple;

    for (int i = 0; i < tupleNum; i++) {
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
    Scanner scanner = sm.getScanner(meta, schema, fragment, target);
    scanner.init();
    int tupleCnt = 0;
    Tuple tuple;
    while ((tuple = scanner.next()) != null) {
      verifyProjectedFields(scanner.isProjectable(), tuple, tupleCnt);
      tupleCnt++;
    }
    scanner.close();

    assertEquals(tupleNum, tupleCnt);
  }

  private void verifyProjectedFields(boolean projectable, Tuple tuple, int tupleCnt) {
    if (projectable) {
      assertTrue(tupleCnt + 2 == tuple.getInt8(0));
      assertTrue(tupleCnt + 3 == tuple.getFloat4(1));
    } else {
      // RAW and ROW always project all fields.
      if (!storeType.equalsIgnoreCase("RAW") && !storeType.equalsIgnoreCase("ROWFILE")) {
        assertTrue(tuple.isBlankOrNull(0));
      }
      assertTrue(tupleCnt + 2 == tuple.getInt8(1));
      assertTrue(tupleCnt + 3 == tuple.getFloat4(2));
    }
  }

  @Test
  public void testVariousTypes() throws IOException {
    boolean handleProtobuf = !storeType.equalsIgnoreCase("JSON");

    Schema schema = new Schema();
    schema.addColumn("col1", Type.BOOLEAN);
    schema.addColumn("col2", Type.CHAR, 7);
    schema.addColumn("col3", Type.INT2);
    schema.addColumn("col4", Type.INT4);
    schema.addColumn("col5", Type.INT8);
    schema.addColumn("col6", Type.FLOAT4);
    schema.addColumn("col7", Type.FLOAT8);
    schema.addColumn("col8", Type.TEXT);
    schema.addColumn("col9", Type.BLOB);
    schema.addColumn("col10", Type.INET4);
    schema.addColumn("col11", Type.NULL_TYPE);
    if (handleProtobuf) {
      schema.addColumn("col12", CatalogUtil.newDataType(Type.PROTOBUF, TajoIdProtos.QueryIdProto.class.getName()));
    }

    KeyValueSet options = new KeyValueSet();
    TableMeta meta = CatalogUtil.newTableMeta(storeType, options);
    meta.setOptions(CatalogUtil.newDefaultProperty(storeType));
    if (storeType.equalsIgnoreCase("AVRO")) {
      String path = FileUtil.getResourcePath("dataset/testVariousTypes.avsc").toString();
      meta.putOption(StorageConstants.AVRO_SCHEMA_URL, path);
    }

    FileTablespace sm = TablespaceManager.getLocalFs();
    Path tablePath = new Path(testDir, "testVariousTypes.data");
    Appender appender = sm.getAppender(meta, schema, tablePath);
    appender.init();

    QueryId queryid = new QueryId("12345", 5);
    ProtobufDatumFactory factory = ProtobufDatumFactory.get(TajoIdProtos.QueryIdProto.class.getName());

    VTuple tuple = new VTuple(11 + (handleProtobuf ? 1 : 0));
    tuple.put(new Datum[] {
        DatumFactory.createBool(true),
        DatumFactory.createChar("hyunsik"),
        DatumFactory.createInt2((short) 17),
        DatumFactory.createInt4(59),
        DatumFactory.createInt8(23l),
        DatumFactory.createFloat4(77.9f),
        DatumFactory.createFloat8(271.9f),
        DatumFactory.createText("hyunsik"),
        DatumFactory.createBlob("hyunsik".getBytes()),
        DatumFactory.createInet4("192.168.0.1"),
        NullDatum.get()
    });
    if (handleProtobuf) {
      tuple.put(11, factory.createDatum(queryid.getProto()));
    }

    appender.addTuple(tuple);
    appender.flush();
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner =  sm.getScanner(meta, schema, fragment);
    scanner.init();

    Tuple retrieved;
    while ((retrieved = scanner.next()) != null) {
      for (int i = 0; i < tuple.size(); i++) {
        assertEquals(tuple.get(i), retrieved.asDatum(i));
      }
    }
    scanner.close();
  }

  @Test
  public void testNullHandlingTypes() throws IOException {
    boolean handleProtobuf = !storeType.equalsIgnoreCase("JSON");

    Schema schema = new Schema();
    schema.addColumn("col1", Type.BOOLEAN);
    schema.addColumn("col2", Type.CHAR, 7);
    schema.addColumn("col3", Type.INT2);
    schema.addColumn("col4", Type.INT4);
    schema.addColumn("col5", Type.INT8);
    schema.addColumn("col6", Type.FLOAT4);
    schema.addColumn("col7", Type.FLOAT8);
    schema.addColumn("col8", Type.TEXT);
    schema.addColumn("col9", Type.BLOB);
    schema.addColumn("col10", Type.INET4);
    schema.addColumn("col11", Type.NULL_TYPE);

    if (handleProtobuf) {
      schema.addColumn("col12", CatalogUtil.newDataType(Type.PROTOBUF, TajoIdProtos.QueryIdProto.class.getName()));
    }

    KeyValueSet options = new KeyValueSet();
    TableMeta meta = CatalogUtil.newTableMeta(storeType, options);
    meta.setOptions(CatalogUtil.newDefaultProperty(storeType));
    meta.putOption(StorageConstants.TEXT_NULL, "\\\\N");
    meta.putOption(StorageConstants.RCFILE_NULL, "\\\\N");
    meta.putOption(StorageConstants.RCFILE_SERDE, TextSerializerDeserializer.class.getName());
    meta.putOption(StorageConstants.SEQUENCEFILE_NULL, "\\");
    if (storeType.equalsIgnoreCase("AVRO")) {
      meta.putOption(StorageConstants.AVRO_SCHEMA_LITERAL,
                     TEST_NULL_HANDLING_TYPES_AVRO_SCHEMA);
    }

    Path tablePath = new Path(testDir, "testVariousTypes.data");
    FileTablespace sm = TablespaceManager.getLocalFs();
    Appender appender = sm.getAppender(meta, schema, tablePath);
    appender.init();

    QueryId queryid = new QueryId("12345", 5);
    ProtobufDatumFactory factory = ProtobufDatumFactory.get(TajoIdProtos.QueryIdProto.class.getName());
    int columnNum = 11 + (handleProtobuf ? 1 : 0);
    VTuple seedTuple = new VTuple(columnNum);
    seedTuple.put(new Datum[]{
        DatumFactory.createBool(true),                // 0
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
    });

    if (handleProtobuf) {
      seedTuple.put(11, factory.createDatum(queryid.getProto()));       // 12
    }

    // Making tuples with different null column positions
    Tuple tuple;
    for (int i = 0; i < columnNum; i++) {
      tuple = new VTuple(columnNum);
      for (int j = 0; j < columnNum; j++) {
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
    Scanner scanner = TablespaceManager.getLocalFs().getScanner(meta, schema, fragment);
    scanner.init();

    Tuple retrieved;
    int i = 0;
    while ((retrieved = scanner.next()) != null) {
      assertEquals(columnNum, retrieved.size());
      for (int j = 0; j < columnNum; j++) {
        if (i == j) {
          assertEquals(NullDatum.get(), retrieved.asDatum(j));
        } else {
          assertEquals(seedTuple.get(j), retrieved.asDatum(j));
        }
      }

      i++;
    }
    scanner.close();
  }

  @Test
  public void testRCFileTextSerializeDeserialize() throws IOException {
    if(!storeType.equalsIgnoreCase("RCFILE")) return;

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

    KeyValueSet options = new KeyValueSet();
    TableMeta meta = CatalogUtil.newTableMeta(storeType, options);
    meta.putOption(StorageConstants.CSVFILE_SERDE, TextSerializerDeserializer.class.getName());

    Path tablePath = new Path(testDir, "testVariousTypes.data");
    FileTablespace sm = TablespaceManager.getLocalFs();
    Appender appender = sm.getAppender(meta, schema, tablePath);
    appender.enableStats();
    appender.init();

    QueryId queryid = new QueryId("12345", 5);
    ProtobufDatumFactory factory = ProtobufDatumFactory.get(TajoIdProtos.QueryIdProto.class.getName());

    VTuple tuple = new VTuple(new Datum[] {
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
    Scanner scanner =  TablespaceManager.getLocalFs().getScanner(meta, schema, fragment);
    scanner.init();

    Tuple retrieved;
    while ((retrieved=scanner.next()) != null) {
      for (int i = 0; i < tuple.size(); i++) {
        assertEquals(tuple.get(i), retrieved.asDatum(i));
      }
    }
    scanner.close();
    assertEquals(appender.getStats().getNumBytes().longValue(), scanner.getInputStats().getNumBytes().longValue());
    assertEquals(appender.getStats().getNumRows().longValue(), scanner.getInputStats().getNumRows().longValue());
  }

  @Test
  public void testRCFileBinarySerializeDeserialize() throws IOException {
    if(!storeType.equalsIgnoreCase("RCFILE")) return;

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

    KeyValueSet options = new KeyValueSet();
    TableMeta meta = CatalogUtil.newTableMeta(storeType, options);
    meta.putOption(StorageConstants.RCFILE_SERDE, BinarySerializerDeserializer.class.getName());

    Path tablePath = new Path(testDir, "testVariousTypes.data");
    FileTablespace sm = TablespaceManager.getLocalFs();
    Appender appender = sm.getAppender(meta, schema, tablePath);
    appender.enableStats();
    appender.init();

    QueryId queryid = new QueryId("12345", 5);
    ProtobufDatumFactory factory = ProtobufDatumFactory.get(TajoIdProtos.QueryIdProto.class.getName());

    VTuple tuple = new VTuple(new Datum[] {
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
    Scanner scanner =  TablespaceManager.getLocalFs().getScanner(meta, schema, fragment);
    scanner.init();

    Tuple retrieved;
    while ((retrieved=scanner.next()) != null) {
      for (int i = 0; i < tuple.size(); i++) {
        assertEquals(tuple.get(i), retrieved.asDatum(i));
      }
    }
    scanner.close();
    assertEquals(appender.getStats().getNumBytes().longValue(), scanner.getInputStats().getNumBytes().longValue());
    assertEquals(appender.getStats().getNumRows().longValue(), scanner.getInputStats().getNumRows().longValue());
  }

  @Test
  public void testSequenceFileTextSerializeDeserialize() throws IOException {
    if(!storeType.equalsIgnoreCase("SEQUENCEFILE")) return;

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

    KeyValueSet options = new KeyValueSet();
    TableMeta meta = CatalogUtil.newTableMeta(storeType, options);
    meta.putOption(StorageConstants.SEQUENCEFILE_SERDE, TextSerializerDeserializer.class.getName());

    Path tablePath = new Path(testDir, "testVariousTypes.data");
    FileTablespace sm = TablespaceManager.getLocalFs();
    Appender appender = sm.getAppender(meta, schema, tablePath);
    appender.enableStats();
    appender.init();

    QueryId queryid = new QueryId("12345", 5);
    ProtobufDatumFactory factory = ProtobufDatumFactory.get(TajoIdProtos.QueryIdProto.class.getName());

    VTuple tuple = new VTuple(new Datum[] {
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
    Scanner scanner =  TablespaceManager.getLocalFs().getScanner(meta, schema, fragment);
    scanner.init();

    assertTrue(scanner instanceof SequenceFileScanner);
    Writable key = ((SequenceFileScanner) scanner).getKey();
    assertEquals(key.getClass().getCanonicalName(), LongWritable.class.getCanonicalName());

    Tuple retrieved;
    while ((retrieved=scanner.next()) != null) {
      for (int i = 0; i < tuple.size(); i++) {
        assertEquals(tuple.get(i), retrieved.asDatum(i));
      }
    }
    scanner.close();
    assertEquals(appender.getStats().getNumBytes().longValue(), scanner.getInputStats().getNumBytes().longValue());
    assertEquals(appender.getStats().getNumRows().longValue(), scanner.getInputStats().getNumRows().longValue());
  }

  @Test
  public void testSequenceFileBinarySerializeDeserialize() throws IOException {
    if(!storeType.equalsIgnoreCase("SEQUENCEFILE")) return;

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

    KeyValueSet options = new KeyValueSet();
    TableMeta meta = CatalogUtil.newTableMeta(storeType, options);
    meta.putOption(StorageConstants.SEQUENCEFILE_SERDE, BinarySerializerDeserializer.class.getName());

    Path tablePath = new Path(testDir, "testVariousTypes.data");
    FileTablespace sm = TablespaceManager.getLocalFs();
    Appender appender = sm.getAppender(meta, schema, tablePath);
    appender.enableStats();
    appender.init();

    QueryId queryid = new QueryId("12345", 5);
    ProtobufDatumFactory factory = ProtobufDatumFactory.get(TajoIdProtos.QueryIdProto.class.getName());

    VTuple tuple = new VTuple(13);
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
    Scanner scanner = TablespaceManager.getLocalFs().getScanner(meta, schema, fragment);
    scanner.init();

    assertTrue(scanner instanceof SequenceFileScanner);
    Writable key = ((SequenceFileScanner) scanner).getKey();
    assertEquals(key.getClass().getCanonicalName(), BytesWritable.class.getCanonicalName());

    Tuple retrieved;
    while ((retrieved=scanner.next()) != null) {
      for (int i = 0; i < tuple.size(); i++) {
        assertEquals(tuple.get(i), retrieved.asDatum(i));
      }
    }
    scanner.close();
    assertEquals(appender.getStats().getNumBytes().longValue(), scanner.getInputStats().getNumBytes().longValue());
    assertEquals(appender.getStats().getNumRows().longValue(), scanner.getInputStats().getNumRows().longValue());
  }

  @Test
  public void testTime() throws IOException {
    if (storeType.equalsIgnoreCase("CSV") || storeType.equalsIgnoreCase("RAW")) {
      Schema schema = new Schema();
      schema.addColumn("col1", Type.DATE);
      schema.addColumn("col2", Type.TIME);
      schema.addColumn("col3", Type.TIMESTAMP);

      KeyValueSet options = new KeyValueSet();
      TableMeta meta = CatalogUtil.newTableMeta(storeType, options);

      Path tablePath = new Path(testDir, "testTime.data");
      FileTablespace sm = TablespaceManager.getLocalFs();
      Appender appender = sm.getAppender(meta, schema, tablePath);
      appender.init();

      VTuple tuple = new VTuple(new Datum[]{
          DatumFactory.createDate("1980-04-01"),
          DatumFactory.createTime("12:34:56"),
          DatumFactory.createTimestmpDatumWithUnixTime((int)(System.currentTimeMillis() / 1000))
      });
      appender.addTuple(tuple);
      appender.flush();
      appender.close();

      FileStatus status = fs.getFileStatus(tablePath);
      FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
      Scanner scanner = TablespaceManager.getLocalFs().getScanner(meta, schema, fragment);
      scanner.init();

      Tuple retrieved;
      while ((retrieved = scanner.next()) != null) {
        for (int i = 0; i < tuple.size(); i++) {
          assertEquals(tuple.get(i), retrieved.asDatum(i));
        }
      }
      scanner.close();
    }
  }

  @Test
  public void testSeekableScanner() throws IOException {
    if (!seekable) {
      return;
    }

    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("age", Type.INT8);
    schema.addColumn("comment", Type.TEXT);

    TableMeta meta = CatalogUtil.newTableMeta(storeType);
    Path tablePath = new Path(testDir, "Seekable.data");
    FileTablespace sm = TablespaceManager.getLocalFs();
    FileAppender appender = (FileAppender) sm.getAppender(meta, schema, tablePath);
    appender.enableStats();
    appender.init();
    int tupleNum = 100000;
    VTuple vTuple;

    List<Long> offsets = Lists.newArrayList();
    offsets.add(0L);
    for (int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(3);
      vTuple.put(0, DatumFactory.createInt4(i + 1));
      vTuple.put(1, DatumFactory.createInt8(25l));
      vTuple.put(2, DatumFactory.createText("test" + i));
      appender.addTuple(vTuple);

      // find a seek position
      if (i % (tupleNum / 3) == 0) {
        offsets.add(appender.getOffset());
      }
    }

    // end of file
    if (!offsets.contains(appender.getOffset())) {
      offsets.add(appender.getOffset());
    }

    appender.close();
    if (statsable) {
      TableStats stat = appender.getStats();
      assertEquals(tupleNum, stat.getNumRows().longValue());
    }

    FileStatus status = fs.getFileStatus(tablePath);
    assertEquals(status.getLen(), appender.getOffset());

    Scanner scanner;
    int tupleCnt = 0;
    long prevOffset = 0;
    long readBytes = 0;
    long readRows = 0;
    for (long offset : offsets) {
      scanner = TablespaceManager.getLocalFs().getScanner(meta, schema,
	        new FileFragment("table", tablePath, prevOffset, offset - prevOffset), schema);
      scanner.init();

      while (scanner.next() != null) {
        tupleCnt++;
      }

      scanner.close();
      if (statsable) {
        readBytes += scanner.getInputStats().getNumBytes();
        readRows += scanner.getInputStats().getNumRows();
      }
      prevOffset = offset;
    }

    assertEquals(tupleNum, tupleCnt);
    if (statsable) {
      assertEquals(appender.getStats().getNumBytes().longValue(), readBytes);
      assertEquals(appender.getStats().getNumRows().longValue(), readRows);
    }
  }

  @Test
  public void testMaxValue() throws IOException {

    Schema schema = new Schema();
    schema.addColumn("col1", Type.FLOAT4);
    schema.addColumn("col2", Type.FLOAT8);
    schema.addColumn("col3", Type.INT2);
    schema.addColumn("col4", Type.INT4);
    schema.addColumn("col5", Type.INT8);

    KeyValueSet options = new KeyValueSet();
    TableMeta meta = CatalogUtil.newTableMeta(storeType, options);
    if (storeType.equalsIgnoreCase("AVRO")) {
      meta.putOption(StorageConstants.AVRO_SCHEMA_LITERAL, TEST_MAX_VALUE_AVRO_SCHEMA);
    }

    if (storeType.equalsIgnoreCase("RAW")) {
      OldStorageManager.clearCache();
      /* TAJO-1250 reproduce BufferOverflow of RAWFile */
      int headerSize = 4 + 2 + 1; //Integer record length + Short null-flag length + 1 byte null flags
      /* max varint32: 5 bytes, max varint64: 10 bytes */
      int record = 4 + 8 + 2 + 5 + 8; // required size is 27
      conf.setInt(RawFile.WRITE_BUFFER_SIZE, record + headerSize);
    }

    FileTablespace sm = TablespaceManager.getLocalFs();
    Path tablePath = new Path(testDir, "testMaxValue.data");
    Appender appender = sm.getAppender(meta, schema, tablePath);

    appender.init();

    VTuple tuple = new VTuple(new Datum[]{
        DatumFactory.createFloat4(Float.MAX_VALUE),
        DatumFactory.createFloat8(Double.MAX_VALUE),
        DatumFactory.createInt2(Short.MAX_VALUE),
        DatumFactory.createInt4(Integer.MAX_VALUE),
        DatumFactory.createInt8(Long.MAX_VALUE)
    });

    appender.addTuple(tuple);
    appender.flush();
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner = sm.getScanner(meta, schema, fragment);
    scanner.init();

    Tuple retrieved;
    while ((retrieved = scanner.next()) != null) {
      for (int i = 0; i < tuple.size(); i++) {
        assertEquals(tuple.get(i), retrieved.asDatum(i));
      }
    }
    scanner.close();


    if (storeType.equalsIgnoreCase("RAW")){
      OldStorageManager.clearCache();
    }
  }

  @Test
  public void testLessThanSchemaSize() throws IOException {
    /* RAW is internal storage. It must be same with schema size */
    if (storeType.equalsIgnoreCase("RAW") || storeType.equalsIgnoreCase("AVRO")
        || storeType.equalsIgnoreCase("PARQUET")){
      return;
    }

    Schema dataSchema = new Schema();
    dataSchema.addColumn("col1", Type.FLOAT4);
    dataSchema.addColumn("col2", Type.FLOAT8);
    dataSchema.addColumn("col3", Type.INT2);

    KeyValueSet options = new KeyValueSet();
    TableMeta meta = CatalogUtil.newTableMeta(storeType, options);
    meta.setOptions(CatalogUtil.newDefaultProperty(storeType));

    Path tablePath = new Path(testDir, "testLessThanSchemaSize.data");
    FileTablespace sm = TablespaceManager.getLocalFs();
    Appender appender = sm.getAppender(meta, dataSchema, tablePath);
    appender.init();


    Tuple expect = new VTuple(dataSchema.size());
    expect.put(new Datum[]{
        DatumFactory.createFloat4(Float.MAX_VALUE),
        DatumFactory.createFloat8(Double.MAX_VALUE),
        DatumFactory.createInt2(Short.MAX_VALUE)
    });

    appender.addTuple(expect);
    appender.flush();
    appender.close();

    assertTrue(fs.exists(tablePath));
    FileStatus status = fs.getFileStatus(tablePath);
    Schema inSchema = new Schema();
    inSchema.addColumn("col1", Type.FLOAT4);
    inSchema.addColumn("col2", Type.FLOAT8);
    inSchema.addColumn("col3", Type.INT2);
    inSchema.addColumn("col4", Type.INT4);
    inSchema.addColumn("col5", Type.INT8);

    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner = TablespaceManager.getLocalFs().getScanner(meta, inSchema, fragment);

    Schema target = new Schema();

    target.addColumn("col2", Type.FLOAT8);
    target.addColumn("col5", Type.INT8);
    scanner.setTarget(target.toArray());
    scanner.init();

    Tuple tuple = scanner.next();
    scanner.close();

    if (scanner.isProjectable()) {
      assertEquals(expect.asDatum(1), tuple.asDatum(0));
      assertEquals(NullDatum.get(), tuple.asDatum(1));
    } else {
      assertEquals(expect.asDatum(1), tuple.asDatum(1));
      assertEquals(NullDatum.get(), tuple.asDatum(4));
    }
  }

  @Test
  public final void testInsertFixedCharTypeWithOverSize() throws Exception {
    if (storeType.equalsIgnoreCase("CSV") == false &&
        storeType.equalsIgnoreCase("SEQUENCEFILE") == false &&
        storeType.equalsIgnoreCase("RCFILE") == false &&
        storeType.equalsIgnoreCase("PARQUET") == false) {
      return;
    }

    Schema dataSchema = new Schema();
    dataSchema.addColumn("col1", Type.CHAR);

    KeyValueSet options = new KeyValueSet();
    TableMeta meta = CatalogUtil.newTableMeta(storeType, options);
    meta.setOptions(CatalogUtil.newDefaultProperty(storeType));

    Path tablePath = new Path(testDir, "test_storetype_oversize.data");
    FileTablespace sm = TablespaceManager.getLocalFs();
    Appender appender = sm.getAppender(meta, dataSchema, tablePath);
    appender.init();

    Tuple expect = new VTuple(dataSchema.size());
    expect.put(new Datum[]{
        DatumFactory.createChar("1"),
    });

    appender.addTuple(expect);
    appender.flush();

    Tuple expect2 = new VTuple(dataSchema.size());
    expect2.put(new Datum[]{
        DatumFactory.createChar("12"),
    });

    boolean ok = false;
    try {
      appender.addTuple(expect2);
      appender.flush();
      appender.close();
    } catch (ValueTooLongForTypeCharactersException e) {
      ok = true;
    }

    assertTrue(ok);
  }
}
