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

/**
 * 
 */
package org.apache.tajo.jdbc;

import com.google.protobuf.ByteString;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.*;
import org.apache.tajo.TajoProtos.CodecType;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.ipc.ClientProtos.SerializedResultSet;
import org.apache.tajo.storage.*;
import org.apache.tajo.tuple.memory.MemoryBlock;
import org.apache.tajo.tuple.memory.MemoryRowBlock;
import org.apache.tajo.util.CompressionUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestResultSet {
  private static TajoTestingCluster util;
  private static TajoConf conf;
  private static TableDesc desc;
  private static FileTablespace sm;
  private static TableMeta scoreMeta;
  private static Schema scoreSchema;
  private static MemoryRowBlock rowBlock;

  @BeforeClass
  public static void setup() throws Exception {
    util = TpchTestBase.getInstance().getTestingCluster();
    conf = util.getConfiguration();
    sm = TablespaceManager.getDefault();

    scoreSchema = SchemaBuilder.builder()
        .add("deptname", Type.TEXT)
        .add("score", Type.INT4)
        .build();

    scoreMeta = CatalogUtil.newTableMeta(BuiltinStorages.TEXT, conf);
    rowBlock = new MemoryRowBlock(SchemaUtil.toDataTypes(scoreSchema));
    TableStats stats = new TableStats();

    Path p = new Path(sm.getTableUri("default", "score"));
    sm.getFileSystem().mkdirs(p);
    Appender appender = sm.getAppender(scoreMeta, scoreSchema, new Path(p, "score"));
    appender.init();

    int deptSize = 100;
    int tupleNum = 10000;
    Tuple tuple;
    long written = 0;
    for (int i = 0; i < tupleNum; i++) {
      tuple = new VTuple(2);
      String key = "test" + (i % deptSize);
      tuple.put(0, DatumFactory.createText(key));
      tuple.put(1, DatumFactory.createInt4(i + 1));
      written += key.length() + Integer.SIZE;
      appender.addTuple(tuple);
      rowBlock.getWriter().addTuple(tuple);
    }
    appender.close();
    stats.setNumRows(tupleNum);
    stats.setNumBytes(written);
    stats.setAvgRows(tupleNum);
    stats.setNumBlocks(1000);
    stats.setNumShuffleOutputs(100);
    desc = new TableDesc(CatalogUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, "score"),
        scoreSchema, scoreMeta, p.toUri());
    desc.setStats(stats);
    assertEquals(tupleNum, rowBlock.rows());
  }

  @AfterClass
  public static void terminate() throws IOException {
    rowBlock.release();
  }

  @Test
  public void testMemoryResultSet() throws Exception {
    SerializedResultSet.Builder resultSetBuilder = SerializedResultSet.newBuilder();
    resultSetBuilder.setSchema(scoreSchema.getProto());
    resultSetBuilder.setRows(rowBlock.rows());

    MemoryBlock memoryBlock = rowBlock.getMemory();
    ByteBuffer uncompressed = memoryBlock.getBuffer().nioBuffer(0, memoryBlock.readableBytes());
    resultSetBuilder.setSerializedTuples(ByteString.copyFrom(uncompressed));
    resultSetBuilder.setDecompressedLength(memoryBlock.readableBytes());

    TajoMemoryResultSet rs = new TajoMemoryResultSet(null, desc.getSchema(), resultSetBuilder.build(), null);

    ResultSetMetaData meta = rs.getMetaData();
    assertNotNull(meta);
    Schema schema = scoreSchema;
    assertEquals(schema.size(), meta.getColumnCount());
    for (int i = 0; i < meta.getColumnCount(); i++) {
      assertEquals(schema.getColumn(i).getSimpleName(), meta.getColumnName(i + 1));
      assertEquals(schema.getColumn(i).getQualifier(), meta.getTableName(i + 1));
    }

    int i = 0;
    assertTrue(rs.isBeforeFirst());
    for (; rs.next(); i++) {
      assertEquals("test"+i%100, rs.getString(1));
      assertEquals("test"+i%100, rs.getString("deptname"));
      assertEquals(i+1, rs.getInt(2));
      assertEquals(i+1, rs.getInt("score"));
    }
    assertEquals(10000, i);
    assertTrue(rs.isAfterLast());
  }


  @Test
  public void testCompressedMemoryResultSet() throws Exception {
    SerializedResultSet.Builder resultSetBuilder = SerializedResultSet.newBuilder();
    resultSetBuilder.setSchema(scoreSchema.getProto());
    resultSetBuilder.setRows(rowBlock.rows());

    MemoryBlock memoryBlock = rowBlock.getMemory();
    byte[] uncompressedBytes = new byte[memoryBlock.readableBytes()];
    memoryBlock.getBuffer().getBytes(0, uncompressedBytes);

    // compress by snappy
    byte[] compressedBytes = CompressionUtil.compress(CodecType.SNAPPY, uncompressedBytes);
    resultSetBuilder.setDecompressedLength(uncompressedBytes.length);
    resultSetBuilder.setDecompressCodec(CodecType.SNAPPY);
    resultSetBuilder.setSerializedTuples(ByteString.copyFrom(compressedBytes));


    TajoMemoryResultSet rs = new TajoMemoryResultSet(null, desc.getSchema(), resultSetBuilder.build(), null);
    ResultSetMetaData meta = rs.getMetaData();
    assertNotNull(meta);
    Schema schema = scoreSchema;
    assertEquals(schema.size(), meta.getColumnCount());
    for (int i = 0; i < meta.getColumnCount(); i++) {
      assertEquals(schema.getColumn(i).getSimpleName(), meta.getColumnName(i + 1));
      assertEquals(schema.getColumn(i).getQualifier(), meta.getTableName(i + 1));
    }

    int i = 0;
    assertTrue(rs.isBeforeFirst());
    for (; rs.next(); i++) {
      assertEquals("test"+i%100, rs.getString(1));
      assertEquals("test"+i%100, rs.getString("deptname"));
      assertEquals(i+1, rs.getInt(2));
      assertEquals(i+1, rs.getInt("score"));
    }
    assertEquals(10000, i);
    assertTrue(rs.isAfterLast());
  }

  @Test
  public void testDateTimeType() throws Exception {
    // HiveCatalog does not support date type, time type in hive-0.12.0
    if(util.isHiveCatalogStoreRunning()) return;

    ResultSet res = null;
    TajoClient client = util.newTajoClient();
    try {
      String tableName = "datetimetable";
      String query = "select col1, col2, col3 from " + tableName;

      String [] table = new String[] {tableName};
      Schema schema = SchemaBuilder.builder()
          .add("col1", Type.DATE)
          .add("col2", Type.TIME)
          .add("col3", Type.TIMESTAMP)
          .build();
      Schema [] schemas = new Schema[] {schema};
      String [] data = {
          "2014-01-01|01:00:00|2014-01-01 01:00:00"
      };

      res = TajoTestingCluster
          .run(table, schemas, new String[][]{data}, query, client);

      assertTrue(res.next());

      Date date = res.getDate(1);
      assertNotNull(date);
      assertEquals(Date.valueOf("2014-01-01"), date);

      date = res.getDate("col1");
      assertNotNull(date);
      assertEquals(Date.valueOf("2014-01-01"), date);

      Time time = res.getTime(2);
      assertNotNull(time);
      assertEquals(Time.valueOf("01:00:00"), time);

      time = res.getTime("col2");
      assertNotNull(time);
      assertEquals(Time.valueOf("01:00:00"), time);

      Timestamp timestamp = res.getTimestamp(3);
      assertNotNull(timestamp);
      assertEquals(Timestamp.valueOf("2014-01-01 01:00:00"), timestamp);

      timestamp = res.getTimestamp("col3");
      assertNotNull(timestamp);
      assertEquals(Timestamp.valueOf("2014-01-01 01:00:00"), timestamp);

      // assert with timezone

      //Current timezone + 1 hour
      TimeZone tz = TimeZone.getDefault();
      tz.setRawOffset(tz.getRawOffset() + (int) TimeUnit.HOURS.toMillis(1));

      Calendar cal = Calendar.getInstance(tz);
      assertEquals(tz.getRawOffset(), cal.getTimeZone().getRawOffset());
      date = res.getDate(1, cal);
      assertNotNull(date);
      assertEquals("2014-01-01", date.toString());

      date = res.getDate("col1", cal);
      assertNotNull(date);
      assertEquals("2014-01-01", date.toString());

      time = res.getTime(2);
      assertNotNull(time);
      assertEquals("01:00:00", time.toString());

      time = res.getTime("col2");
      assertNotNull(time);
      assertEquals("01:00:00", time.toString());

      timestamp = res.getTimestamp(3, cal);
      assertNotNull(timestamp);
      assertEquals("2014-01-01 02:00:00.0", timestamp.toString());

      timestamp = res.getTimestamp("col3", cal);
      assertNotNull(timestamp);
      assertEquals("2014-01-01 02:00:00.0", timestamp.toString());
    } finally {
      if (res != null) {
        res.close();
      }

      client.close();
    }
  }
}
