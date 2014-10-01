/*
 * Lisensed to the Apache Software Foundation (ASF) under one
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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.tajo.QueryId;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
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
import org.apache.tajo.storage.sequencefile.SequenceFileScanner;
import org.apache.tajo.tuple.RowBlockReader;
import org.apache.tajo.tuple.offheap.OffHeapRowBlock;
import org.apache.tajo.tuple.offheap.ZeroCopyTuple;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.KeyValueSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestNextFetches {
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
      "    { \"name\": \"col2\", \"type\": [\"null\", \"int\"] },\n" +
      "    { \"name\": \"col3\", \"type\": [\"null\", \"string\"] },\n" +
      "    { \"name\": \"col4\", \"type\": [\"null\", \"int\"] },\n" +
      "    { \"name\": \"col5\", \"type\": [\"null\", \"int\"] },\n" +
      "    { \"name\": \"col6\", \"type\": [\"null\", \"long\"] },\n" +
      "    { \"name\": \"col7\", \"type\": [\"null\", \"float\"] },\n" +
      "    { \"name\": \"col8\", \"type\": [\"null\", \"double\"] },\n" +
      "    { \"name\": \"col9\", \"type\": [\"null\", \"string\"] },\n" +
      "    { \"name\": \"col10\", \"type\": [\"null\", \"bytes\"] },\n" +
      "    { \"name\": \"col11\", \"type\": [\"null\", \"bytes\"] },\n" +
      "    { \"name\": \"col12\", \"type\": \"null\" },\n" +
      "    { \"name\": \"col13\", \"type\": [\"null\", \"bytes\"] }\n" +
      "  ]\n" +
      "}\n";

  private Schema schema;

  private StoreType storeType;
  private boolean splitable;
  private boolean statsable;
  private Path testDir;
  private FileSystem fs;
  private Tuple allTypedTuple;

  public TestNextFetches(StoreType type, boolean splitable, boolean statsable) throws IOException {
    this.storeType = type;
    this.splitable = splitable;
    this.statsable = statsable;

    conf = new TajoConf();

    if (storeType == StoreType.RCFILE) {
      conf.setInt(RCFile.RECORD_INTERVAL_CONF_STR, 100);
    }

    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    fs = testDir.getFileSystem(conf);

    schema = new Schema();
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
    if (storeType == StoreType.RAW) {
      schema.addColumn("col12", CatalogUtil.newDataType(Type.PROTOBUF, TajoIdProtos.QueryIdProto.class.getName()));
    }

    QueryId queryid = new QueryId("12345", 5);
    ProtobufDatumFactory factory = ProtobufDatumFactory.get(TajoIdProtos.QueryIdProto.class.getName());
    int columnNum = 11 + (storeType == StoreType.RAW ? 1 : 0);
    allTypedTuple = new VTuple(columnNum);
    allTypedTuple.put(new Datum[]{
        DatumFactory.createBool(true),
        DatumFactory.createChar("jinho"),
        DatumFactory.createInt2((short) 17),
        DatumFactory.createInt4(59),
        DatumFactory.createInt8(23l),
        DatumFactory.createFloat4(77.9f),
        DatumFactory.createFloat8(271.9f),
        DatumFactory.createText("jinho"),
        DatumFactory.createBlob("jinho babo".getBytes()),
        DatumFactory.createInet4("192.168.0.1"),
        NullDatum.get(),
    });
    if (storeType == StoreType.RAW) {
      allTypedTuple.put(11, factory.createDatum(queryid.getProto()));
    }
  }

  @Parameterized.Parameters
  public static Collection<Object[]> generateParameters() {
    return Arrays.asList(new Object[][] {
        {StoreType.CSV, true, true},
        // TODO - to be implemented
//        {StoreType.RAW, false, false},
//        {StoreType.RCFILE, true, true},
        {StoreType.BLOCK_PARQUET, false, false},
//        {StoreType.SEQUENCEFILE, true, true},
//        {StoreType.AVRO, false, false},
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

      Scanner scanner = StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, tablets[0], schema);
      assertTrue(scanner.isSplittable());
      scanner.init();
      int tupleCnt = 0;

      OffHeapRowBlock rowBlock = new OffHeapRowBlock(schema, 64 * StorageUnit.KB);
      rowBlock.setMaxRow(1024);

      while (scanner.nextFetch(rowBlock)) {
        tupleCnt += rowBlock.rows();
      }
      scanner.close();

      scanner = StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, tablets[1], schema);
      assertTrue(scanner.isSplittable());
      scanner.init();
      while (scanner.nextFetch(rowBlock)) {
        tupleCnt += rowBlock.rows();
      }
      scanner.close();

      assertEquals(tupleNum, tupleCnt);

      rowBlock.release();
    }
	}

  @Test
  public void testSplitableForRCFileBug() throws IOException {
    if (storeType == StoreType.RCFILE) {
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

      Scanner scanner = StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, tablets[0], schema);
      assertTrue(scanner.isSplittable());
      scanner.init();
      int tupleCnt = 0;

      OffHeapRowBlock rowBlock = new OffHeapRowBlock(schema, 64 * StorageUnit.KB);
      rowBlock.setMaxRow(1024);

      while (scanner.nextFetch(rowBlock)) {
        tupleCnt += rowBlock.rows();
      }
      scanner.close();

      scanner = StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, tablets[1], schema);
      assertTrue(scanner.isSplittable());
      scanner.init();
      while (scanner.nextFetch(rowBlock)) {
        tupleCnt += rowBlock.rows();
      }
      scanner.close();

      assertEquals(tupleNum, tupleCnt);

      rowBlock.release();
    }
  }

  @Test
  public void testProjection() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("age", Type.INT8);
    schema.addColumn("score", Type.FLOAT4);

    TableMeta meta = CatalogUtil.newTableMeta(storeType);
    meta.setOptions(StorageUtil.newPhysicalProperties(storeType));
    if (storeType == StoreType.AVRO) {
      meta.putOption(StorageConstants.AVRO_SCHEMA_LITERAL,
                     TEST_PROJECTION_AVRO_SCHEMA);
    }

    Path tablePath = new Path(testDir, "testProjection.data");
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, schema, tablePath);
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
    Scanner scanner = StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, fragment, target);
    scanner.init();
    int tupleCnt = 0;

    OffHeapRowBlock rowBlock = new OffHeapRowBlock(schema, 64 * StorageUnit.KB);
    rowBlock.setMaxRow(1024);

    ZeroCopyTuple tuple = new ZeroCopyTuple();
    while (scanner.nextFetch(rowBlock)) {
      RowBlockReader reader = rowBlock.getReader();
      while (reader.next(tuple)) {
        if (storeType == StoreType.RCFILE
            || storeType == StoreType.TREVNI
            || storeType == StoreType.CSV
            || storeType == StoreType.PARQUET
            || storeType == StoreType.BLOCK_PARQUET
            || storeType == StoreType.SEQUENCEFILE
            || storeType == StoreType.AVRO) {
          assertTrue(tuple.isNull(0));
        }
        assertTrue(tuple.toString(), tupleCnt + 2 == tuple.getInt8(1));
        assertTrue(tuple.toString(), tupleCnt + 3 == tuple.getFloat4(2));
        tupleCnt++;
      }
    }
    scanner.close();

    assertEquals(tupleNum, tupleCnt);

    rowBlock.release();
  }

  @Test
  public void testVariousTypes() throws IOException {
    KeyValueSet options = new KeyValueSet();
    TableMeta meta = CatalogUtil.newTableMeta(storeType, options);
    meta.setOptions(StorageUtil.newPhysicalProperties(storeType));
    if (storeType == StoreType.AVRO) {
      String path = FileUtil.getResourcePath("testVariousTypes.avsc").toString();
      meta.putOption(StorageConstants.AVRO_SCHEMA_URL, path);
    }

    Path tablePath = new Path(testDir, "testVariousTypes.data");
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, schema, tablePath);
    appender.init();
    appender.addTuple(allTypedTuple);
    appender.flush();
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner =  StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, fragment);
    scanner.init();

    OffHeapRowBlock rowBlock = new OffHeapRowBlock(schema, 64 * StorageUnit.KB);
    rowBlock.setMaxRow(1024);

    ZeroCopyTuple zcTuple = new ZeroCopyTuple();
    while (scanner.nextFetch(rowBlock)) {
      RowBlockReader reader = rowBlock.getReader();
      while (reader.next(zcTuple)) {
        for (int i = 0; i < allTypedTuple.size(); i++) {
          if (schema.getColumn(i).getDataType().getType() == Type.CHAR) {
            assertEquals(i + "th column is different.",
                allTypedTuple.get(i).asChars().trim(), zcTuple.get(i).asChars().trim());
          } else {
            assertEquals(i + "th column is different.", allTypedTuple.get(i), zcTuple.get(i));
          }

        }
      }
    }
    scanner.close();

    rowBlock.release();
  }

  @Test
  public void testNullHandlingTypes() throws IOException {
    KeyValueSet options = new KeyValueSet();
    TableMeta meta = CatalogUtil.newTableMeta(storeType, options);
    meta.setOptions(StorageUtil.newPhysicalProperties(storeType));
    meta.putOption(StorageConstants.CSVFILE_NULL, "\\\\N");
    meta.putOption(StorageConstants.RCFILE_NULL, "\\\\N");
    meta.putOption(StorageConstants.RCFILE_SERDE, TextSerializerDeserializer.class.getName());
    meta.putOption(StorageConstants.SEQUENCEFILE_NULL, "\\");
    if (storeType == StoreType.AVRO) {
      meta.putOption(StorageConstants.AVRO_SCHEMA_LITERAL,
                     TEST_NULL_HANDLING_TYPES_AVRO_SCHEMA);
    }

    Path tablePath = new Path(testDir, "testVariousTypes.data");
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, schema, tablePath);
    appender.init();

    int columnNum = allTypedTuple.size();
    // Making tuples with different null column positions
    Tuple tuple;
    for (int i = 0; i < columnNum; i++) {
      tuple = new VTuple(columnNum);
      for (int j = 0; j < columnNum; j++) {
        if (i == j) { // i'th column will have NULL value
          tuple.put(j, NullDatum.get());
        } else {
          tuple.put(j, allTypedTuple.get(j));
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

    OffHeapRowBlock rowBlock = new OffHeapRowBlock(schema, 64 * StorageUnit.KB);
    rowBlock.setMaxRow(1024);

    ZeroCopyTuple retrieved = new ZeroCopyTuple();

    int i = 0;
    while (scanner.nextFetch(rowBlock)) {
      RowBlockReader reader = rowBlock.getReader();

      while(reader.next(retrieved)) {
        assertEquals(columnNum, retrieved.size());
        for (int j = 0; j < columnNum; j++) {
          if (i == j) {
            assertEquals(NullDatum.get(), retrieved.get(j));
          } else {
            if (schema.getColumn(j).getDataType().getType() == Type.CHAR) {
              assertEquals(allTypedTuple.get(j).asChars().trim(), retrieved.get(j).asChars().trim());
            } else {
              assertEquals(allTypedTuple.get(j), retrieved.get(j));
            }
          }
        }

        i++;
      }
    }
    scanner.close();

    rowBlock.release();
  }

  @Test
  public void testRCFileTextSerializeDeserialize() throws IOException {
    if(storeType != StoreType.RCFILE) return;

    KeyValueSet options = new KeyValueSet();
    TableMeta meta = CatalogUtil.newTableMeta(storeType, options);
    meta.putOption(StorageConstants.CSVFILE_SERDE, TextSerializerDeserializer.class.getName());

    Path tablePath = new Path(testDir, "testVariousTypes.data");
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, schema, tablePath);
    appender.enableStats();
    appender.init();

    QueryId queryid = new QueryId("12345", 5);
    ProtobufDatumFactory factory = ProtobufDatumFactory.get(TajoIdProtos.QueryIdProto.class.getName());

    int columnNum = allTypedTuple.size();
    appender.addTuple(allTypedTuple);
    appender.flush();
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    assertEquals(appender.getStats().getNumBytes().longValue(), status.getLen());

    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner =  StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, fragment);
    scanner.init();

    OffHeapRowBlock rowBlock = new OffHeapRowBlock(schema, 64 * StorageUnit.KB);
    rowBlock.setMaxRow(1024);

    ZeroCopyTuple retrieved = new ZeroCopyTuple();
    while (scanner.nextFetch(rowBlock)) {
      RowBlockReader reader = rowBlock.getReader();
      while (reader.next(retrieved)) {
        for (int i = 0; i < allTypedTuple.size(); i++) {
          assertEquals(allTypedTuple.get(i), retrieved.get(i));
        }
      }
    }
    scanner.close();
    assertEquals(appender.getStats().getNumBytes().longValue(), scanner.getInputStats().getNumBytes().longValue());
    assertEquals(appender.getStats().getNumRows().longValue(), scanner.getInputStats().getNumRows().longValue());

    rowBlock.release();
  }

  @Test
  public void testRCFileBinarySerializeDeserialize() throws IOException {
    if(storeType != StoreType.RCFILE) return;

    KeyValueSet options = new KeyValueSet();
    TableMeta meta = CatalogUtil.newTableMeta(storeType, options);
    meta.putOption(StorageConstants.RCFILE_SERDE, BinarySerializerDeserializer.class.getName());

    Path tablePath = new Path(testDir, "testVariousTypes.data");
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, schema, tablePath);
    appender.enableStats();
    appender.init();
    appender.addTuple(allTypedTuple);
    appender.flush();
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    assertEquals(appender.getStats().getNumBytes().longValue(), status.getLen());

    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner =  StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, fragment);
    scanner.init();

    OffHeapRowBlock rowBlock = new OffHeapRowBlock(schema, 64 * StorageUnit.KB);
    rowBlock.setMaxRow(1024);

    ZeroCopyTuple retrieved = new ZeroCopyTuple();
    while (scanner.nextFetch(rowBlock)) {
      RowBlockReader reader = rowBlock.getReader();
      while (reader.next(retrieved)) {
        for (int i = 0; i < allTypedTuple.size(); i++) {
          assertEquals(allTypedTuple.get(i), retrieved.get(i));
        }
      }
    }
    scanner.close();
    assertEquals(appender.getStats().getNumBytes().longValue(), scanner.getInputStats().getNumBytes().longValue());
    assertEquals(appender.getStats().getNumRows().longValue(), scanner.getInputStats().getNumRows().longValue());

    rowBlock.release();
  }

  @Test
  public void testSequenceFileTextSerializeDeserialize() throws IOException {
    if(storeType != StoreType.SEQUENCEFILE) return;

    KeyValueSet options = new KeyValueSet();
    TableMeta meta = CatalogUtil.newTableMeta(storeType, options);
    meta.putOption(StorageConstants.SEQUENCEFILE_SERDE, TextSerializerDeserializer.class.getName());

    Path tablePath = new Path(testDir, "testVariousTypes.data");
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, schema, tablePath);
    appender.enableStats();
    appender.init();
    appender.addTuple(allTypedTuple);
    appender.flush();
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    assertEquals(appender.getStats().getNumBytes().longValue(), status.getLen());

    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner =  StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, fragment);
    scanner.init();

    assertTrue(scanner instanceof SequenceFileScanner);
    Writable key = ((SequenceFileScanner) scanner).getKey();
    assertEquals(key.getClass().getCanonicalName(), LongWritable.class.getCanonicalName());

    OffHeapRowBlock rowBlock = new OffHeapRowBlock(schema, 64 * StorageUnit.KB);
    rowBlock.setMaxRow(1024);

    ZeroCopyTuple retrieved = new ZeroCopyTuple();

    while (scanner.nextFetch(rowBlock)) {
      RowBlockReader reader = rowBlock.getReader();
      while (reader.next(retrieved)) {
        for (int i = 0; i < allTypedTuple.size(); i++) {
          assertEquals(allTypedTuple.get(i), retrieved.get(i));
        }
      }
    }
    scanner.close();
    assertEquals(appender.getStats().getNumBytes().longValue(), scanner.getInputStats().getNumBytes().longValue());
    assertEquals(appender.getStats().getNumRows().longValue(), scanner.getInputStats().getNumRows().longValue());

    rowBlock.release();
  }

  @Test
  public void testSequenceFileBinarySerializeDeserialize() throws IOException {
    if(storeType != StoreType.SEQUENCEFILE) return;

    KeyValueSet options = new KeyValueSet();
    TableMeta meta = CatalogUtil.newTableMeta(storeType, options);
    meta.putOption(StorageConstants.SEQUENCEFILE_SERDE, BinarySerializerDeserializer.class.getName());

    Path tablePath = new Path(testDir, "testVariousTypes.data");
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, schema, tablePath);
    appender.enableStats();
    appender.init();

    appender.addTuple(allTypedTuple);
    appender.flush();
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    assertEquals(appender.getStats().getNumBytes().longValue(), status.getLen());

    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner =  StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, fragment);
    scanner.init();

    assertTrue(scanner instanceof SequenceFileScanner);
    Writable key = ((SequenceFileScanner) scanner).getKey();
    assertEquals(key.getClass().getCanonicalName(), BytesWritable.class.getCanonicalName());

    OffHeapRowBlock rowBlock = new OffHeapRowBlock(schema, 64 * StorageUnit.KB);
    rowBlock.setMaxRow(1024);

    ZeroCopyTuple retrieved = new ZeroCopyTuple();

    while (scanner.nextFetch(rowBlock)) {
      RowBlockReader reader = rowBlock.getReader();
      while (reader.next(retrieved)) {
        for (int i = 0; i < allTypedTuple.size(); i++) {
          assertEquals(allTypedTuple.get(i), retrieved.get(i));
        }
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

      KeyValueSet options = new KeyValueSet();
      TableMeta meta = CatalogUtil.newTableMeta(storeType, options);

      Path tablePath = new Path(testDir, "testTime.data");
      Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, schema, tablePath);
      appender.init();

      Tuple tuple = new VTuple(3);
      tuple.put(new Datum[]{
          DatumFactory.createDate("1980-04-01"),
          DatumFactory.createTime("12:34:56"),
          DatumFactory.createTimestmpDatumWithUnixTime((int)(System.currentTimeMillis() / 1000))
      });
      appender.addTuple(tuple);
      appender.flush();
      appender.close();

      FileStatus status = fs.getFileStatus(tablePath);
      FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
      Scanner scanner = StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, fragment);
      scanner.init();

      OffHeapRowBlock rowBlock = new OffHeapRowBlock(schema, 64 * StorageUnit.KB);
      rowBlock.setMaxRow(1024);

      ZeroCopyTuple retrieved = new ZeroCopyTuple();

      while (scanner.nextFetch(rowBlock)) {
        RowBlockReader reader = rowBlock.getReader();
        while (reader.next(retrieved)) {
          for (int i = 0; i < tuple.size(); i++) {
            assertEquals(tuple.get(i), retrieved.get(i));
          }
        }
      }
      scanner.close();

      rowBlock.release();
    }
  }

}
