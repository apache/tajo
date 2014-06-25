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

import org.apache.hadoop.fs.Path;
import org.apache.tajo.IntegrationTest;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.*;
import org.apache.tajo.util.KeyValueSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.sql.*;
import java.util.Calendar;
import java.util.TimeZone;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestResultSet {
  private static TajoTestingCluster util;
  private static TajoConf conf;
  private static TableDesc desc;
  private static AbstractStorageManager sm;
  private static TableMeta scoreMeta;
  private static Schema scoreSchema;

  @BeforeClass
  public static void setup() throws Exception {
    util = TpchTestBase.getInstance().getTestingCluster();
    conf = util.getConfiguration();
    sm = StorageManagerFactory.getStorageManager(conf);

    scoreSchema = new Schema();
    scoreSchema.addColumn("deptname", Type.TEXT);
    scoreSchema.addColumn("score", Type.INT4);
    scoreMeta = CatalogUtil.newTableMeta(StoreType.CSV);
    TableStats stats = new TableStats();

    Path p = sm.getTablePath("score");
    sm.getFileSystem().mkdirs(p);
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(scoreMeta, scoreSchema,
        new Path(p, "score"));
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
    }
    appender.close();
    stats.setNumRows(tupleNum);
    stats.setNumBytes(written);
    stats.setAvgRows(tupleNum);
    stats.setNumBlocks(1000);
    stats.setNumShuffleOutputs(100);
    desc = new TableDesc(CatalogUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, "score"),
        scoreSchema, scoreMeta, p);
    desc.setStats(stats);
  }

  @AfterClass
  public static void terminate() throws IOException {

  }

  @Test
  public void test() throws IOException, SQLException {
    TajoResultSet rs = new TajoResultSet(null, null, conf, desc);
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
    TimeZone tajoCurrentTimeZone = TajoConf.getCurrentTimeZone();
    TajoConf.setCurrentTimeZone(TimeZone.getTimeZone("UTC"));

    TimeZone systemCurrentTimeZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

    ResultSet res = null;
    try {
      String tableName = "datetimetable";
      String query = "select col1, col2, col3 from " + tableName;

      String [] table = new String[] {tableName};
      Schema schema = new Schema();
      schema.addColumn("col1", Type.DATE);
      schema.addColumn("col2", Type.TIME);
      schema.addColumn("col3", Type.TIMESTAMP);
      Schema [] schemas = new Schema[] {schema};
      String [] data = {
          "2014-01-01|01:00:00|2014-01-01 01:00:00"
      };
      KeyValueSet tableOptions = new KeyValueSet();
      tableOptions.put(StorageConstants.CSVFILE_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);

      res = TajoTestingCluster
          .run(table, schemas, tableOptions, new String[][]{data}, query);

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
      Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+9"));
      date = res.getDate(1, cal);
      assertNotNull(date);
      assertEquals("2014-01-01", date.toString());

      date = res.getDate("col1", cal);
      assertNotNull(date);
      assertEquals("2014-01-01", date.toString());

      time = res.getTime(2, cal);
      assertNotNull(time);
      assertEquals("10:00:00", time.toString());

      time = res.getTime("col2", cal);
      assertNotNull(time);
      assertEquals("10:00:00", time.toString());

      timestamp = res.getTimestamp(3, cal);
      assertNotNull(timestamp);
      assertEquals("2014-01-01 10:00:00.0", timestamp.toString());

      timestamp = res.getTimestamp("col3", cal);
      assertNotNull(timestamp);
      assertEquals("2014-01-01 10:00:00.0", timestamp.toString());
    } finally {
      TajoConf.setCurrentTimeZone(tajoCurrentTimeZone);
      TimeZone.setDefault(systemCurrentTimeZone);
      res.close();
    }
  }
}
