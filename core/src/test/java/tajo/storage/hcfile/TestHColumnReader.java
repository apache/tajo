/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.storage.hcfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tajo.TajoTestingUtility;
import tajo.catalog.Column;
import tajo.catalog.Schema;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableMeta;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.io.IOException;

import static org.junit.Assert.*;

public class TestHColumnReader {
  private TajoTestingUtility util;
  private Configuration conf;
  private int i, tupleNum = 150000;
  private Path tablePath = new Path("hdfs:///customer");


  @Before
  public void setup() throws Exception {
    util = new TajoTestingUtility();
    util.startMiniDFSCluster(3);
    conf = util.getConfiguration();

    Schema schema = new Schema(new Column[]{new Column("id", DataType.INT)});
    TableMeta tableMeta = TCatUtil.newTableMeta(schema, StoreType.HCFILE);

    HCTupleAppender appender = new HCTupleAppender(conf, tableMeta, schema.getColumn(0), tablePath);
    Tuple tuple = new VTuple(1);

    for (i = 0; i < tupleNum; i++) {
      tuple.put(0, DatumFactory.createInt(i));
      appender.addTuple(tuple);
    }

    appender.close();
  }

  @After
  public void teardown() throws Exception {
    util.shutdownMiniDFSCluster();
  }

  @Test
  public void testSeqscan() throws IOException {

    Datum datum;
    HColumnReader reader = new HColumnReader(conf, new Path(tablePath, "id"));
    for (i = 0; (datum=reader.get()) != null; i++) {
      assertEquals(i, datum.asInt());
    }

    reader.close();

    assertEquals(i, tupleNum);
  }

  @Test
  public void testRandscan() throws IOException {
    Datum datum;
    HColumnReader reader = new HColumnReader(conf, new Path(tablePath, "id"));
    reader.pos(1000);
    for (i = 1000; (datum=reader.get()) != null; i++) {
      assertEquals(i, datum.asInt());
    }
    assertEquals(i, tupleNum);

    reader.pos(3000);
    for (i = 3000; i < 50000; i++) {
      datum = reader.get();
      assertEquals(i, datum.asInt());
    }
    assertEquals(50000, i);

    reader.pos(30000);
    for (i = 30000; (datum=reader.get()) != null; i++) {
      assertEquals(i, datum.asInt());
    }
    assertEquals(i, tupleNum);

    reader.close();
  }
}
