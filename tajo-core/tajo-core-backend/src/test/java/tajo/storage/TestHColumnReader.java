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

package tajo.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tajo.TajoTestingCluster;
import tajo.catalog.Column;
import tajo.catalog.Schema;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableMeta;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.storage.hcfile.HCTupleAppender;
import tajo.storage.hcfile.HColumnReader;
import tajo.util.FileUtil;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestHColumnReader {
  private TajoTestingCluster util;
  private Configuration conf;
  private int i, tupleNum = 150000;
  private Path tablePath = new Path("hdfs:///customer");


  @Before
  public void setup() throws Exception {
    util = new TajoTestingCluster();
    util.startMiniDFSCluster(1);
    conf = util.getConfiguration();

    Schema schema = new Schema(
        new Column[]{
            new Column("id", DataType.INT),
            new Column("name", DataType.STRING2)});
    TableMeta tableMeta = TCatUtil.newTableMeta(schema, StoreType.HCFILE);
    FileUtil.writeProto(util.getDefaultFileSystem(),
        new Path(tablePath, ".meta"), tableMeta.getProto());

    HCTupleAppender appender = new HCTupleAppender(conf, tableMeta, 1, tablePath);
    Tuple tuple = new VTuple(2);

    for (i = 0; i < tupleNum; i++) {
      tuple.put(0, DatumFactory.createInt(i));
      tuple.put(1, DatumFactory.createString2("abcdefghijklmnopqrstuvwxyz"));
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
    HColumnReader reader = new HColumnReader(conf, tablePath, "id");
    for (i = 0; (datum=reader.get()) != null; i++) {
      assertEquals(i, datum.asInt());
    }

    reader.close();

    assertEquals(i, tupleNum);

    reader = new HColumnReader(conf, tablePath, "name");
    for (i = 0; (datum=reader.get()) != null; i++) {
      assertEquals("abcdefghijklmnopqrstuvwxyz", datum.asChars());
    }

    reader.close();

    assertEquals(i, tupleNum);
  }

  @Test
  public void testRandscan() throws IOException {
    Datum datum;
    HColumnReader idReader = new HColumnReader(conf, tablePath, 0);
    HColumnReader nameReader = new HColumnReader(conf, tablePath, "name");
    idReader.pos(100000);
    nameReader.pos(100000);
    for (i = 100000; (datum=idReader.get()) != null; i++) {
      assertEquals(i, datum.asInt());
      assertEquals("abcdefghijklmnopqrstuvwxyz", nameReader.get().asChars());
    }
    assertEquals(i, tupleNum);

    idReader.pos(3000);
    nameReader.pos(3000);
    for (i = 3000; i < 50000; i++) {
      datum = idReader.get();
      assertEquals(i, datum.asInt());
      assertEquals("abcdefghijklmnopqrstuvwxyz", nameReader.get().asChars());
    }
    assertEquals(50000, i);

    idReader.pos(30000);
    nameReader.pos(30000);
    for (i = 30000; (datum=idReader.get()) != null; i++) {
      assertEquals(i, datum.asInt());
      assertEquals("abcdefghijklmnopqrstuvwxyz", nameReader.get().asChars());
    }
    assertEquals(i, tupleNum);

    idReader.pos(0);
    nameReader.pos(0);
    for (i = 0; (datum=idReader.get()) != null; i++) {
      assertEquals(i, datum.asInt());
      assertEquals("abcdefghijklmnopqrstuvwxyz", nameReader.get().asChars());
    }
    assertEquals(i, tupleNum);

    idReader.close();
  }
}

