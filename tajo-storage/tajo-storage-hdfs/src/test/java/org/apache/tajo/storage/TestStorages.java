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
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.QueryId;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaBuilder;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.ProtobufDatumFactory;
import org.apache.tajo.exception.ValueTooLongForTypeCharactersException;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.rcfile.RCFile;
import org.apache.tajo.storage.sequencefile.SequenceFileScanner;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.JavaResourceUtil;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TestStorages {
  private TajoConf conf;

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

  private static String TEST_VARIABLE_LENGTH_AVRO_SCHEMA =
      "{\n" +
          "  \"type\": \"record\",\n" +
          "  \"namespace\": \"org.apache.tajo\",\n" +
          "  \"name\": \"testVariableLength\",\n" +
          "  \"fields\": [\n" +
          "    { \"name\": \"col1\", \"type\": \"string\" }\n" +
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
      "    { \"name\": \"col11\", \"type\": [\"null\", \"bytes\"] }\n" +
      "  ]\n" +
      "}\n";

  private static String TEST_EMPTY_FILED_AVRO_SCHEMA =
      "{\n" +
          "  \"type\": \"record\",\n" +
          "  \"namespace\": \"org.apache.tajo\",\n" +
          "  \"name\": \"testEmptySchema\",\n" +
          "  \"fields\": []\n" +
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

  private String dataFormat;
  private boolean splitable;
  private boolean statsable;
  private boolean seekable;
  private boolean internalType;
  private Path testDir;
  private FileSystem fs;

  public TestStorages(String type, boolean splitable, boolean statsable, boolean seekable, boolean internalType)
      throws IOException {
    final String TEST_PATH = "target/test-data/TestStorages";

    this.dataFormat = type;
    this.splitable = splitable;
    this.statsable = statsable;
    this.seekable = seekable;
    this.internalType = internalType;
    conf = new TajoConf();

    if (dataFormat.equalsIgnoreCase(BuiltinStorages.RCFILE)) {
      conf.setInt(RCFile.RECORD_INTERVAL_CONF_STR, 100);
    }

    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    fs = testDir.getFileSystem(conf);
  }

  @Parameters(name = "{index}: {0}")
  public static Collection<Object[]> generateParameters() {
    return Arrays.asList(new Object[][] {
        //type, splitable, statsable, seekable, internalType
        {BuiltinStorages.RAW, false, true, true, true},
        {BuiltinStorages.DRAW, false, true, true, true},
        {BuiltinStorages.RCFILE, true, true, false, false},
        {BuiltinStorages.PARQUET, false, false, false, false},
        {BuiltinStorages.ORC, false, true, false, false},
        {BuiltinStorages.SEQUENCE_FILE, true, true, false, false},
        {BuiltinStorages.AVRO, false, false, false, false},
        {BuiltinStorages.TEXT, true, true, true, false},
        {BuiltinStorages.JSON, true, true, false, false},
    });
  }

  @After
  public void tearDown() throws IOException {
   fs.delete(testDir, true);
  }

  private boolean protoTypeSupport() {
    return internalType;
  }

  private boolean timeTypeSupport() {
    return internalType
        || dataFormat.equalsIgnoreCase(BuiltinStorages.TEXT);
  }

  private boolean dateTypeSupport() {
    return internalType
        || dataFormat.equalsIgnoreCase(BuiltinStorages.TEXT)
        || dataFormat.equalsIgnoreCase(BuiltinStorages.PARQUET)
        || dataFormat.equalsIgnoreCase(BuiltinStorages.ORC);
  }

  @Test
  public void testSplitable() throws IOException {
    if (splitable) {
      Schema schema = SchemaBuilder.builder()
          .add("id", Type.INT4)
          .add("age", Type.INT8)
          .build();

      TableMeta meta = CatalogUtil.newTableMeta(dataFormat, conf);
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
  public void testZeroRows() throws IOException {
    Schema schema = SchemaBuilder.builder()
        .add("id", Type.INT4)
        .add("age", Type.INT8)
        .add("score", Type.FLOAT4)
        .build();

    TableMeta meta = CatalogUtil.newTableMeta(dataFormat, conf);
    if (dataFormat.equalsIgnoreCase(BuiltinStorages.AVRO)) {
      meta.putProperty(StorageConstants.AVRO_SCHEMA_LITERAL,
          TEST_PROJECTION_AVRO_SCHEMA);
    }

    Path tablePath = new Path(testDir, "testZeroRows.data");
    FileTablespace sm = TablespaceManager.getLocalFs();
    Appender appender = sm.getAppender(meta, schema, tablePath);
    appender.enableStats();
    appender.init();
    appender.close();

    TableStats stat = appender.getStats();
    assertEquals(0, stat.getNumRows().longValue());

    if(internalType || BuiltinStorages.TEXT.equals(dataFormat)) {
      FileStatus fileStatus = fs.getFileStatus(tablePath);
      assertEquals(0, fileStatus.getLen());
    }

    List<Fragment> splits = sm.getSplits("testZeroRows", meta, schema, false, testDir);
    int tupleCnt = 0;
    for (Fragment fragment : splits) {
      Scanner scanner = sm.getScanner(meta, schema, fragment, schema);
      scanner.init();
      while (scanner.next() != null) {
        tupleCnt++;
      }
      scanner.close();
    }

    assertEquals(0, tupleCnt);
  }

  @Test
  public void testRCFileSplitable() throws IOException {
    if (dataFormat.equalsIgnoreCase(BuiltinStorages.RCFILE)) {
      Schema schema = SchemaBuilder.builder()
          .add("id", Type.INT4)
          .add("age", Type.INT8)
          .build();

      TableMeta meta = CatalogUtil.newTableMeta(dataFormat, conf);
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
    Schema schema = SchemaBuilder.builder()
        .add("id", Type.INT4)
        .add("age", Type.INT8)
        .add("score", Type.FLOAT4)
        .build();

    TableMeta meta = CatalogUtil.newTableMeta(dataFormat, conf);
    if (dataFormat.equalsIgnoreCase(BuiltinStorages.AVRO)) {
      meta.putProperty(StorageConstants.AVRO_SCHEMA_LITERAL,
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

    Schema target = SchemaBuilder.builder()
        .add("age", Type.INT8)
        .add("score", Type.FLOAT4)
        .build();
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
      // Internal storage always project all fields.
      if (!internalType) {
        assertTrue(tuple.isBlankOrNull(0));
      }
      assertTrue(tupleCnt + 2 == tuple.getInt8(1));
      assertTrue(tupleCnt + 3 == tuple.getFloat4(2));
    }
  }

  @Test
  public void testVariousTypes() throws IOException {
    SchemaBuilder schemaBld = SchemaBuilder.builder()
        .add("col1", Type.BOOLEAN)
        .add("col2", CatalogUtil.newDataTypeWithLen(Type.CHAR, 7))
        .add("col3", Type.INT2)
        .add("col4", Type.INT4)
        .add("col5", Type.INT8)
        .add("col6", Type.FLOAT4)
        .add("col7", Type.FLOAT8)
        .add("col8", Type.TEXT)
        .add("col9", Type.BLOB);
    if (dateTypeSupport()) {
      schemaBld.add("col10", Type.DATE);
    }
    if (protoTypeSupport()) {
      schemaBld.add("col11", CatalogUtil.newDataType(Type.PROTOBUF, TajoIdProtos.QueryIdProto.class.getName()));
    }

    Schema schema = schemaBld.build();

    TableMeta meta = CatalogUtil.newTableMeta(dataFormat, conf);
    if (dataFormat.equalsIgnoreCase(BuiltinStorages.AVRO)) {
      String path = JavaResourceUtil.getResourceURL("dataset/testVariousTypes.avsc").toString();
      meta.putProperty(StorageConstants.AVRO_SCHEMA_URL, path);
    }

    FileTablespace sm = TablespaceManager.getLocalFs();
    Path tablePath = new Path(testDir, "testVariousTypes.data");
    Appender appender = sm.getAppender(meta, schema, tablePath);
    appender.init();

    QueryId queryid = new QueryId("12345", 5);

    VTuple tuple = new VTuple(9 + (dateTypeSupport() ? 1 : 0) + (protoTypeSupport() ? 1 : 0));
    tuple.put(new Datum[] {
        DatumFactory.createBool(true),
        DatumFactory.createChar("hyunsik"),
        DatumFactory.createInt2((short) 17),
        DatumFactory.createInt4(59),
        DatumFactory.createInt8(23L),
        DatumFactory.createFloat4(77.9f),
        DatumFactory.createFloat8(271.9f),
        DatumFactory.createText("hyunsik"),
        DatumFactory.createBlob("hyunsik".getBytes()),
    });

    short currentIdx = 9;

    if (dateTypeSupport()) {
      tuple.put(currentIdx, DatumFactory.createDate(2016, 6, 28));
      currentIdx++;
    }

    if (protoTypeSupport()) {
      tuple.put(currentIdx, ProtobufDatumFactory.createDatum(queryid.getProto()));
    }

    appender.addTuple(tuple);
    appender.flush();
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner =  sm.getScanner(meta, schema, fragment, null);
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
  public void testVariableLength() throws IOException {
    SchemaBuilder schemaBld = SchemaBuilder.builder()
        .add("col1", Type.TEXT);

    Schema schema = schemaBld.build();

    TableMeta meta = CatalogUtil.newTableMeta(dataFormat, conf);
    if (dataFormat.equalsIgnoreCase(BuiltinStorages.AVRO)) {
      meta.putProperty(StorageConstants.AVRO_SCHEMA_LITERAL, TEST_VARIABLE_LENGTH_AVRO_SCHEMA);
    }

    FileTablespace sm = TablespaceManager.getLocalFs();
    Path tablePath = new Path(testDir, "testVariableLength.data");
    Appender appender = sm.getAppender(meta, schema, tablePath);
    appender.init();

    String testStr = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";
    String testStr2 = "xxxxxxxxxxxxxxxx";  // test for dictionary encoding

    for(int i = 100; i > 0; i--) {
      VTuple tuple = new VTuple(1 );
      tuple.put(new Datum[] {
          DatumFactory.createText(i % 2 == 0 ? testStr + i : testStr2)
      });

      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner =  sm.getScanner(meta, schema, fragment, null);
    scanner.init();

    Tuple retrieved;
    int idx = 100;
    while ((retrieved = scanner.next()) != null) {
      assertEquals((idx % 2 == 0 ? testStr + idx : testStr2), retrieved.asDatum(0).toString());
      idx--;
    }
    scanner.close();
  }

  @Test
  public void testNullHandlingTypes() throws IOException {
    SchemaBuilder schemaBld = SchemaBuilder.builder()
        .add("col1", Type.BOOLEAN)
        .add("col2", CatalogUtil.newDataTypeWithLen(Type.CHAR, 7))
        .add("col3", Type.INT2)
        .add("col4", Type.INT4)
        .add("col5", Type.INT8)
        .add("col6", Type.FLOAT4)
        .add("col7", Type.FLOAT8)
        .add("col8", Type.TEXT)
        .add("col9", Type.BLOB);

    if (protoTypeSupport()) {
      schemaBld.add("col10", CatalogUtil.newDataType(Type.PROTOBUF, TajoIdProtos.QueryIdProto.class.getName()));
    }

    Schema schema = schemaBld.build();

    TableMeta meta = CatalogUtil.newTableMeta(dataFormat, conf);
    meta.putProperty(StorageConstants.TEXT_NULL, "\\\\N");
    meta.putProperty(StorageConstants.RCFILE_NULL, "\\\\N");
    meta.putProperty(StorageConstants.RCFILE_SERDE, TextSerializerDeserializer.class.getName());
    meta.putProperty(StorageConstants.SEQUENCEFILE_NULL, "\\");
    if (dataFormat.equalsIgnoreCase("AVRO")) {
      meta.putProperty(StorageConstants.AVRO_SCHEMA_LITERAL, TEST_NULL_HANDLING_TYPES_AVRO_SCHEMA);
    }

    Path tablePath = new Path(testDir, "testVariousTypes.data");
    FileTablespace sm = TablespaceManager.getLocalFs();
    Appender appender = sm.getAppender(meta, schema, tablePath);
    appender.init();

    QueryId queryid = new QueryId("12345", 5);
    ProtobufDatumFactory factory = ProtobufDatumFactory.get(TajoIdProtos.QueryIdProto.class.getName());
    int columnNum = 9 + (protoTypeSupport() ? 1 : 0);
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
    });

    if (protoTypeSupport()) {
      seedTuple.put(9, factory.createDatum(queryid.getProto()));       // 10
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
    Scanner scanner = TablespaceManager.getLocalFs().getScanner(meta, schema, fragment, null);
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
  public void testNullHandlingTypesWithProjection() throws IOException {
    if (internalType) return;

    SchemaBuilder schemaBld = SchemaBuilder.builder()
    .add("col1", Type.BOOLEAN)
    .add("col2", CatalogUtil.newDataTypeWithLen(Type.CHAR, 7))
    .add("col3", Type.INT2)
    .add("col4", Type.INT4)
    .add("col5", Type.INT8)
    .add("col6", Type.FLOAT4)
    .add("col7", Type.FLOAT8)
    .add("col8", Type.TEXT)
    .add("col9", Type.BLOB);

    if (protoTypeSupport()) {
      schemaBld.add("col10", CatalogUtil.newDataType(Type.PROTOBUF, TajoIdProtos.QueryIdProto.class.getName()));
    }

    Schema schema = schemaBld.build();

    TableMeta meta = CatalogUtil.newTableMeta(dataFormat, conf);
    meta.putProperty(StorageConstants.TEXT_NULL, "\\\\N");
    meta.putProperty(StorageConstants.RCFILE_NULL, "\\\\N");
    meta.putProperty(StorageConstants.RCFILE_SERDE, TextSerializerDeserializer.class.getName());
    meta.putProperty(StorageConstants.SEQUENCEFILE_NULL, "\\");
    if (dataFormat.equalsIgnoreCase("AVRO")) {
      meta.putProperty(StorageConstants.AVRO_SCHEMA_LITERAL, TEST_NULL_HANDLING_TYPES_AVRO_SCHEMA);
    }

    Path tablePath = new Path(testDir, "testProjectedNullHandlingTypes.data");
    FileTablespace sm = TablespaceManager.getLocalFs();
    Appender appender = sm.getAppender(meta, schema, tablePath);
    appender.init();

    QueryId queryid = new QueryId("12345", 5);
    ProtobufDatumFactory factory = ProtobufDatumFactory.get(TajoIdProtos.QueryIdProto.class.getName());
    int columnNum = 9 + (protoTypeSupport() ? 1 : 0);
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
    });

    if (protoTypeSupport()) {
      seedTuple.put(9, factory.createDatum(queryid.getProto()));       // 10
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


    // Making projection schema with different column positions
    SchemaBuilder targetBld = SchemaBuilder.builder();
    Random random = new Random();
    for (int i = 1; i < schema.size(); i++) {
      int num = random.nextInt(schema.size() - 1) + 1;
      if (i % num == 0) {
        targetBld.add(schema.getColumn(i));
      }
    }
    Schema target = targetBld.build();

    FileStatus status = fs.getFileStatus(tablePath);
    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner = TablespaceManager.getLocalFs().getScanner(meta, schema, fragment, target);
    scanner.init();

    Tuple retrieved;
    int[] targetIds = PlannerUtil.getTargetIds(schema, target.toArray());
    int i = 0;
    while ((retrieved = scanner.next()) != null) {
      assertEquals(target.size(), retrieved.size());
      for (int j = 0; j < targetIds.length; j++) {
        if (i == targetIds[j]) {
          assertEquals(NullDatum.get(), retrieved.asDatum(j));
        } else {
          assertEquals(seedTuple.get(targetIds[j]), retrieved.asDatum(j));
        }
      }
      i++;
    }
    scanner.close();
  }

  @Test
  public void testRCFileTextSerializeDeserialize() throws IOException {
    if(!dataFormat.equalsIgnoreCase(BuiltinStorages.RCFILE)) return;

    Schema schema = SchemaBuilder.builder()
        .add("col1", Type.BOOLEAN)
        .add("col2", CatalogUtil.newDataTypeWithLen(Type.CHAR, 7))
        .add("col3", Type.INT2)
        .add("col4", Type.INT4)
        .add("col5", Type.INT8)
        .add("col6", Type.FLOAT4)
        .add("col7", Type.FLOAT8)
        .add("col8", Type.TEXT)
        .add("col9", Type.BLOB)
        .add("col10", CatalogUtil.newDataType(Type.PROTOBUF, TajoIdProtos.QueryIdProto.class.getName()))
        .build();

    TableMeta meta = CatalogUtil.newTableMeta(dataFormat, conf);
    meta.putProperty(StorageConstants.CSVFILE_SERDE, TextSerializerDeserializer.class.getName());

    Path tablePath = new Path(testDir, "testRCFileTextSerializeDeserialize.data");
    FileTablespace sm = TablespaceManager.getLocalFs();
    Appender appender = sm.getAppender(meta, schema, tablePath);
    appender.enableStats();
    appender.init();

    QueryId queryid = new QueryId("12345", 5);
    ProtobufDatumFactory factory = ProtobufDatumFactory.get(TajoIdProtos.QueryIdProto.class.getName());

    VTuple tuple = new VTuple(new Datum[] {
        DatumFactory.createBool(true),
        DatumFactory.createChar("jinho"),
        DatumFactory.createInt2((short) 17),
        DatumFactory.createInt4(59),
        DatumFactory.createInt8(23l),
        DatumFactory.createFloat4(77.9f),
        DatumFactory.createFloat8(271.9f),
        DatumFactory.createText("jinho"),
        DatumFactory.createBlob("hyunsik babo".getBytes()),
        factory.createDatum(queryid.getProto())
    });
    appender.addTuple(tuple);
    appender.flush();
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    assertEquals(appender.getStats().getNumBytes().longValue(), status.getLen());

    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner =  TablespaceManager.getLocalFs().getScanner(meta, schema, fragment, null);
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
    if(!dataFormat.equalsIgnoreCase(BuiltinStorages.RCFILE)) return;

    Schema schema = SchemaBuilder.builder()
        .add("col1", Type.BOOLEAN)
        .add("col2", CatalogUtil.newDataTypeWithLen(Type.CHAR, 7))
        .add("col3", Type.INT2)
        .add("col4", Type.INT4)
        .add("col5", Type.INT8)
        .add("col6", Type.FLOAT4)
        .add("col7", Type.FLOAT8)
        .add("col8", Type.TEXT)
        .add("col9", Type.BLOB)
        .add("col10", CatalogUtil.newDataType(Type.PROTOBUF, TajoIdProtos.QueryIdProto.class.getName()))
        .build();

    TableMeta meta = CatalogUtil.newTableMeta(dataFormat, conf);
    meta.putProperty(StorageConstants.RCFILE_SERDE, BinarySerializerDeserializer.class.getName());

    Path tablePath = new Path(testDir, "testRCFileBinarySerializeDeserialize.data");
    FileTablespace sm = TablespaceManager.getLocalFs();
    Appender appender = sm.getAppender(meta, schema, tablePath);
    appender.enableStats();
    appender.init();

    QueryId queryid = new QueryId("12345", 5);
    ProtobufDatumFactory factory = ProtobufDatumFactory.get(TajoIdProtos.QueryIdProto.class.getName());

    VTuple tuple = new VTuple(new Datum[] {
        DatumFactory.createBool(true),
        DatumFactory.createChar("jinho"),
        DatumFactory.createInt2((short) 17),
        DatumFactory.createInt4(59),
        DatumFactory.createInt8(23l),
        DatumFactory.createFloat4(77.9f),
        DatumFactory.createFloat8(271.9f),
        DatumFactory.createText("jinho"),
        DatumFactory.createBlob("hyunsik babo".getBytes()),
        factory.createDatum(queryid.getProto())
    });
    appender.addTuple(tuple);
    appender.flush();
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    assertEquals(appender.getStats().getNumBytes().longValue(), status.getLen());

    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner =  TablespaceManager.getLocalFs().getScanner(meta, schema, fragment, null);
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
    if(!dataFormat.equalsIgnoreCase(BuiltinStorages.SEQUENCE_FILE)) return;

    Schema schema = SchemaBuilder.builder()
        .add("col1", Type.BOOLEAN)
        .add("col2", CatalogUtil.newDataTypeWithLen(Type.CHAR, 7))
        .add("col3", Type.INT2)
        .add("col4", Type.INT4)
        .add("col5", Type.INT8)
        .add("col6", Type.FLOAT4)
        .add("col7", Type.FLOAT8)
        .add("col8", Type.TEXT)
        .add("col9", Type.BLOB)
        .add("col10", CatalogUtil.newDataType(Type.PROTOBUF, TajoIdProtos.QueryIdProto.class.getName())).build();

    TableMeta meta = CatalogUtil.newTableMeta(dataFormat, conf);
    meta.putProperty(StorageConstants.SEQUENCEFILE_SERDE, TextSerializerDeserializer.class.getName());

    Path tablePath = new Path(testDir, "testSequenceFileTextSerializeDeserialize.data");
    FileTablespace sm = TablespaceManager.getLocalFs();
    Appender appender = sm.getAppender(meta, schema, tablePath);
    appender.enableStats();
    appender.init();

    QueryId queryid = new QueryId("12345", 5);

    VTuple tuple = new VTuple(new Datum[] {
        DatumFactory.createBool(true),
        DatumFactory.createChar("jinho"),
        DatumFactory.createInt2((short) 17),
        DatumFactory.createInt4(59),
        DatumFactory.createInt8(23l),
        DatumFactory.createFloat4(77.9f),
        DatumFactory.createFloat8(271.9f),
        DatumFactory.createText("jinho"),
        DatumFactory.createBlob("hyunsik babo".getBytes()),
        ProtobufDatumFactory.createDatum(queryid.getProto())
    });
    appender.addTuple(tuple);
    appender.flush();
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    assertEquals(appender.getStats().getNumBytes().longValue(), status.getLen());

    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner =  TablespaceManager.getLocalFs().getScanner(meta, schema, fragment, null);
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
  public void testSequenceFileTextSerializeDeserializeWithDeprecatedProperties() throws IOException {
    if(!dataFormat.equalsIgnoreCase(BuiltinStorages.SEQUENCE_FILE)) return;

    Schema schema = SchemaBuilder.builder()
      .add("col1", Type.BOOLEAN)
      .add("col2", CatalogUtil.newDataTypeWithLen(Type.CHAR, 7))
      .add("col3", Type.INT2)
      .add("col4", Type.INT4)
      .add("col5", Type.INT8)
      .add("col6", Type.FLOAT4)
      .add("col7", Type.FLOAT8)
      .add("col8", Type.TEXT)
      .add("col9", Type.BLOB)
      .add("col10", CatalogUtil.newDataType(Type.PROTOBUF, TajoIdProtos.QueryIdProto.class.getName())).build();

    TableMeta meta = CatalogUtil.newTableMeta(dataFormat, conf);
    meta.putProperty(StorageConstants.SEQUENCEFILE_SERDE, TextSerializerDeserializer.class.getName());
    meta.putProperty(StorageConstants.SEQUENCEFILE_DELIMITER, "\u0001");
    meta.putProperty(StorageConstants.SEQUENCEFILE_NULL, "\\");

    Path tablePath = new Path(testDir, "testSequenceFileTextSerializeDeserializeWithDeprecatedProperties.data");
    FileTablespace sm = TablespaceManager.getLocalFs();
    Appender appender = sm.getAppender(meta, schema, tablePath);
    appender.enableStats();
    appender.init();

    QueryId queryid = new QueryId("12345", 5);

    VTuple tuple = new VTuple(new Datum[] {
      DatumFactory.createBool(true),
      DatumFactory.createChar("jinho"),
      DatumFactory.createInt2((short) 17),
      DatumFactory.createInt4(59),
      DatumFactory.createInt8(23l),
      DatumFactory.createFloat4(77.9f),
      DatumFactory.createFloat8(271.9f),
      DatumFactory.createText("jinho"),
      DatumFactory.createBlob("hyunsik babo".getBytes()),
      ProtobufDatumFactory.createDatum(queryid.getProto())
    });
    appender.addTuple(tuple);
    appender.flush();
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    assertEquals(appender.getStats().getNumBytes().longValue(), status.getLen());

    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner =  TablespaceManager.getLocalFs().getScanner(meta, schema, fragment, null);
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
  public void testSequenceFileTextSerializeDeserializeWithNonDeprecatedProperties() throws IOException {
    if(!dataFormat.equalsIgnoreCase(BuiltinStorages.SEQUENCE_FILE)) return;

    Schema schema = SchemaBuilder.builder()
      .add("col1", Type.BOOLEAN)
      .add("col2", CatalogUtil.newDataTypeWithLen(Type.CHAR, 7))
      .add("col3", Type.INT2)
      .add("col4", Type.INT4)
      .add("col5", Type.INT8)
      .add("col6", Type.FLOAT4)
      .add("col7", Type.FLOAT8)
      .add("col8", Type.TEXT)
      .add("col9", Type.BLOB)
      .add("col10", CatalogUtil.newDataType(Type.PROTOBUF, TajoIdProtos.QueryIdProto.class.getName())).build();

    TableMeta meta = CatalogUtil.newTableMeta(dataFormat, conf);
    meta.putProperty(StorageConstants.SEQUENCEFILE_SERDE, TextSerializerDeserializer.class.getName());
    meta.putProperty(StorageConstants.TEXT_DELIMITER, "\u0001");
    meta.putProperty(StorageConstants.TEXT_NULL, "\\");

    Path tablePath = new Path(testDir, "testSequenceFileTextSerializeDeserializeWithNonDeprecatedProperties.data");
    FileTablespace sm = TablespaceManager.getLocalFs();
    Appender appender = sm.getAppender(meta, schema, tablePath);
    appender.enableStats();
    appender.init();

    QueryId queryid = new QueryId("12345", 5);

    VTuple tuple = new VTuple(new Datum[] {
      DatumFactory.createBool(true),
      DatumFactory.createChar("jinho"),
      DatumFactory.createInt2((short) 17),
      DatumFactory.createInt4(59),
      DatumFactory.createInt8(23l),
      DatumFactory.createFloat4(77.9f),
      DatumFactory.createFloat8(271.9f),
      DatumFactory.createText("jinho"),
      DatumFactory.createBlob("hyunsik babo".getBytes()),
      ProtobufDatumFactory.createDatum(queryid.getProto())
    });
    appender.addTuple(tuple);
    appender.flush();
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    assertEquals(appender.getStats().getNumBytes().longValue(), status.getLen());

    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner =  TablespaceManager.getLocalFs().getScanner(meta, schema, fragment, null);
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
    if(!dataFormat.equalsIgnoreCase(BuiltinStorages.SEQUENCE_FILE)) return;

    Schema schema = SchemaBuilder.builder()
        .add("col1", Type.BOOLEAN)
        .add("col2", CatalogUtil.newDataTypeWithLen(Type.CHAR, 7))
        .add("col3", Type.INT2)
        .add("col4", Type.INT4)
        .add("col5", Type.INT8)
        .add("col6", Type.FLOAT4)
        .add("col7", Type.FLOAT8)
        .add("col8", Type.TEXT)
        .add("col9", Type.BLOB)
        .add("col10", CatalogUtil.newDataType(Type.PROTOBUF, TajoIdProtos.QueryIdProto.class.getName()))
        .build();

    TableMeta meta = CatalogUtil.newTableMeta(dataFormat, conf);
    meta.putProperty(StorageConstants.SEQUENCEFILE_SERDE, BinarySerializerDeserializer.class.getName());

    Path tablePath = new Path(testDir, "testVariousTypes.data");
    FileTablespace sm = TablespaceManager.getLocalFs();
    Appender appender = sm.getAppender(meta, schema, tablePath);
    appender.enableStats();
    appender.init();

    QueryId queryid = new QueryId("12345", 5);

    VTuple tuple = new VTuple(new Datum[] {
        DatumFactory.createBool(true),
        DatumFactory.createChar("jinho"),
        DatumFactory.createInt2((short) 17),
        DatumFactory.createInt4(59),
        DatumFactory.createInt8(23l),
        DatumFactory.createFloat4(77.9f),
        DatumFactory.createFloat8(271.9f),
        DatumFactory.createText("jinho"),
        DatumFactory.createBlob("hyunsik babo".getBytes()),
        ProtobufDatumFactory.createDatum(queryid.getProto())
    });
    appender.addTuple(tuple);
    appender.flush();
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    assertEquals(appender.getStats().getNumBytes().longValue(), status.getLen());

    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner = TablespaceManager.getLocalFs().getScanner(meta, schema, fragment, null);
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
    if (dateTypeSupport() || timeTypeSupport()) {

      int index = 1;
      SchemaBuilder schema = SchemaBuilder.builder();
      if (dateTypeSupport()) {
        schema.add("col" + index++, Type.DATE);
      }
      if (timeTypeSupport()) {
        schema.add("col" + index++, Type.TIMESTAMP);
        schema.add("col" + index++, Type.TIME);
      }

      TableMeta meta = CatalogUtil.newTableMeta(dataFormat, conf);
      assertTrue(meta.containsProperty(StorageConstants.TIMEZONE));

      Path tablePath = new Path(testDir, "testTime.data");
      FileTablespace sm = TablespaceManager.getLocalFs();
      Appender appender = sm.getAppender(meta, schema.build(), tablePath);
      appender.init();

      VTuple tuple = new VTuple(index - 1);
      index = 0;
      if (dateTypeSupport()) {
        tuple.put(index++, DatumFactory.createDate("1980-04-01"));
      }
      if (timeTypeSupport()) {
        tuple.put(index++, DatumFactory.createTimestampDatumWithUnixTime((int)(System.currentTimeMillis() / 1000)));
        tuple.put(index, DatumFactory.createTime("12:34:56"));
      }
      appender.addTuple(tuple);
      appender.flush();
      appender.close();

      FileStatus status = fs.getFileStatus(tablePath);
      FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
      Scanner scanner = TablespaceManager.getLocalFs().getScanner(meta, schema.build(), fragment, null);
      scanner.init();

      Tuple retrieved;
      while ((retrieved = scanner.next()) != null) {
        for (int i = 0; i < tuple.size(); i++) {
          assertEquals("failed at " + i + " th column", tuple.get(i), retrieved.asDatum(i));
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

    Schema schema = SchemaBuilder.builder()
        .add("id", Type.INT4)
        .add("age", Type.INT8)
        .add("comment", Type.TEXT)
        .build();

    TableMeta meta = CatalogUtil.newTableMeta(dataFormat, conf);
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

    Schema schema = SchemaBuilder.builder()
        .add("col1", Type.FLOAT4)
        .add("col2", Type.FLOAT8)
        .add("col3", Type.INT2)
        .add("col4", Type.INT4)
        .add("col5", Type.INT8)
        .build();

    TableMeta meta = CatalogUtil.newTableMeta(dataFormat, conf);
    if (dataFormat.equalsIgnoreCase(BuiltinStorages.AVRO)) {
      meta.putProperty(StorageConstants.AVRO_SCHEMA_LITERAL, TEST_MAX_VALUE_AVRO_SCHEMA);
    }

    if (dataFormat.equalsIgnoreCase(BuiltinStorages.RAW)) {
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
    Scanner scanner = sm.getScanner(meta, schema, fragment, null);
    scanner.init();

    Tuple retrieved;
    while ((retrieved = scanner.next()) != null) {
      for (int i = 0; i < tuple.size(); i++) {
        assertEquals(tuple.get(i), retrieved.asDatum(i));
      }
    }
    scanner.close();


    if (internalType){
      OldStorageManager.clearCache();
    }
  }

  @Test
  public void testLessThanSchemaSize() throws IOException {
    /* Internal storage must be same with schema size */
    if (internalType || dataFormat.equalsIgnoreCase(BuiltinStorages.AVRO)
        || dataFormat.equalsIgnoreCase(BuiltinStorages.ORC)) {
      return;
    }

    Schema dataSchema = SchemaBuilder.builder()
        .add("col1", Type.FLOAT4)
        .add("col2", Type.FLOAT8)
        .add("col3", Type.INT2)
        .build();

    TableMeta meta = CatalogUtil.newTableMeta(dataFormat, conf);

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
    Schema inSchema = SchemaBuilder.builder()
        .add("col1", Type.FLOAT4)
        .add("col2", Type.FLOAT8)
        .add("col3", Type.INT2)
        .add("col4", Type.INT4)
        .add("col5", Type.INT8)
        .build();

    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner = TablespaceManager.getLocalFs().getScanner(meta, inSchema, fragment, null);

    Schema target = SchemaBuilder.builder()
        .add("col2", Type.FLOAT8)
        .add("col5", Type.INT8)
        .build();
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
    if (!dataFormat.equalsIgnoreCase(BuiltinStorages.TEXT) &&
        !dataFormat.equalsIgnoreCase(BuiltinStorages.SEQUENCE_FILE) &&
        !dataFormat.equalsIgnoreCase(BuiltinStorages.RCFILE) &&
        !dataFormat.equalsIgnoreCase(BuiltinStorages.PARQUET)) {
      return;
    }

    Schema dataSchema = SchemaBuilder.builder()
        .add("col1", Type.CHAR)
        .build();

    TableMeta meta = CatalogUtil.newTableMeta(dataFormat, conf);

    Path tablePath = new Path(testDir, "test_dataformat_oversize.data");
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

  @Test
  public void testDateTextHandling() throws Exception {
    if (dataFormat.equalsIgnoreCase(BuiltinStorages.AVRO) || internalType) {
      return;
    }

    Schema schema = SchemaBuilder.builder().add("col1", Type.TEXT).build();

    TableMeta meta = CatalogUtil.newTableMeta(dataFormat, conf);

    FileTablespace sm = TablespaceManager.getLocalFs();
    Path tablePath = new Path(testDir, "testTextHandling.data");

    Appender appender = sm.getAppender(meta, schema, tablePath);

    appender.init();

    VTuple tuple = new VTuple(1);
    tuple.put(0, DatumFactory.createDate(1994,7,30));

    appender.addTuple(tuple);
    appender.flush();
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner = sm.getScanner(meta, schema, fragment, null);
    scanner.init();

    Tuple retrieved;
    while ((retrieved = scanner.next()) != null) {
      assertEquals(tuple.get(0).asChars(), retrieved.asDatum(0).asChars());
    }
    scanner.close();

    if (internalType){
      OldStorageManager.clearCache();
    }
  }

  @Test
  public void testFileAlreadyExists() throws IOException {

    if (internalType) return;

    Schema schema = SchemaBuilder.builder()
        .add("id", Type.INT4)
        .add("age", Type.INT8)
        .add("score", Type.FLOAT4)
        .build();

    TableMeta meta = CatalogUtil.newTableMeta(dataFormat, conf);
    if (dataFormat.equalsIgnoreCase(BuiltinStorages.AVRO)) {
      meta.putProperty(StorageConstants.AVRO_SCHEMA_LITERAL,
          TEST_PROJECTION_AVRO_SCHEMA);
    }

    FileTablespace sm = TablespaceManager.getLocalFs();
    Path tablePath = new Path(testDir, "testFileAlreadyExists.data");

    Appender appender = sm.getAppender(meta, schema, tablePath);
    appender.init();
    appender.close();

    try {
      appender = sm.getAppender(meta, schema, tablePath);
      appender.init();
      if (BuiltinStorages.ORC.equals(dataFormat)) {
        appender.close();
      }
      fail(dataFormat);
    } catch (IOException e) {
    } finally {
      IOUtils.cleanup(null, appender);
    }
  }

  @Test
  public void testProgress() throws IOException {

    Schema schema = SchemaBuilder.builder()
        .add("col1", Type.FLOAT4)
        .add("col2", Type.FLOAT8)
        .add("col3", Type.INT2)
        .add("col4", Type.INT4)
        .add("col5", Type.INT8)
        .build();

    TableMeta meta = CatalogUtil.newTableMeta(dataFormat, conf);
    if (dataFormat.equalsIgnoreCase(BuiltinStorages.AVRO)) {
      meta.putProperty(StorageConstants.AVRO_SCHEMA_LITERAL, TEST_MAX_VALUE_AVRO_SCHEMA);
    }

    FileTablespace sm = TablespaceManager.getLocalFs();
    Path tablePath = new Path(testDir, "testProgress.data");
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
    Scanner scanner =  sm.getScanner(meta, schema, fragment, null);

    assertEquals(0.0f, scanner.getProgress(), 0.0f);

    scanner.init();
    assertNotNull(scanner.next());
    assertNull(null, scanner.next());

    scanner.close();
    assertEquals(1.0f, scanner.getProgress(), 0.0f);
  }

  @Test
  public void testEmptySchema() throws IOException {
    if (internalType) return;

    Schema schema = SchemaBuilder.builder()
        .add("id", Type.INT4)
        .add("age", Type.INT8)
        .add("score", Type.FLOAT4)
        .build();

    TableMeta meta = CatalogUtil.newTableMeta(dataFormat, conf);
    if (dataFormat.equalsIgnoreCase(BuiltinStorages.AVRO)) {
      meta.putProperty(StorageConstants.AVRO_SCHEMA_LITERAL,
          TEST_PROJECTION_AVRO_SCHEMA);
    }

    Path tablePath = new Path(testDir, "testEmptySchema.data");
    FileTablespace sm = TablespaceManager.getLocalFs();
    Appender appender = sm.getAppender(meta, schema, tablePath);
    appender.init();


    Tuple expect = new VTuple(schema.size());
    expect.put(new Datum[]{
        DatumFactory.createInt4(Integer.MAX_VALUE),
        DatumFactory.createInt8(Long.MAX_VALUE),
        DatumFactory.createFloat4(Float.MAX_VALUE)
    });

    appender.addTuple(expect);
    appender.flush();
    appender.close();

    assertTrue(fs.exists(tablePath));
    FileStatus status = fs.getFileStatus(tablePath);

    if (dataFormat.equalsIgnoreCase(BuiltinStorages.AVRO)) {
      meta.putProperty(StorageConstants.AVRO_SCHEMA_LITERAL,
          TEST_EMPTY_FILED_AVRO_SCHEMA);
    }

    //e,g select count(*) from table
    Schema target = SchemaBuilder.builder().build();
    assertEquals(0, target.size());

    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner = TablespaceManager.getLocalFs().getScanner(meta, schema, fragment, target);
    scanner.init();

    Tuple tuple = scanner.next();
    assertNotNull(tuple);
    assertEquals(0, tuple.size());
    scanner.close();
  }
}