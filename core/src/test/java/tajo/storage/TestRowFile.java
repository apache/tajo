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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tajo.TajoTestingUtility;
import tajo.catalog.Schema;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableMeta;
import tajo.catalog.TableMetaImpl;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.catalog.proto.CatalogProtos.TableProto;
import tajo.catalog.statistics.TableStat;
import tajo.conf.TajoConf.ConfVars;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.ipc.protocolrecords.Fragment;
import tajo.util.FileUtil;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.util.Random;

import static org.junit.Assert.*;

public class TestRowFile {
  private TajoTestingUtility util;
  private Configuration conf;
  private RowFile rowFile;

  @Before
  public void setup() throws Exception {
    util = new TajoTestingUtility();
    conf = util.getConfiguration();
    conf.setInt(ConfVars.RAWFILE_SYNC_INTERVAL.varname, 100);
    util.startMiniDFSCluster(3);
    rowFile = new RowFile(conf);
  }

  @After
  public void teardown() throws Exception {
    util.shutdownMiniDFSCluster();
  }

  @Test
  public void test() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("id", DataType.INT);
    schema.addColumn("age", DataType.LONG);
    schema.addColumn("description", DataType.STRING);

    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.RAW);

    Path tablePath = new Path("hdfs:///test");
    Path metaPath = new Path(tablePath, ".meta");
    Path dataDir = new Path(tablePath, "data");
    Path dataPath = new Path(dataDir, "test.tbl");
    FileSystem fs = tablePath.getFileSystem(conf);
    fs.mkdirs(tablePath);
    fs.mkdirs(dataDir);

    FileUtil.writeProto(conf, metaPath, meta.getProto());

    Appender appender = rowFile.getAppender(meta, dataPath);

    int tupleNum = 100000;
    Tuple tuple = null, finalTuple;
    Datum stringDatum = DatumFactory.createString("abcdefghijklmnopqrstuvwxyz");

    long start = System.currentTimeMillis();
    for(int i = 0; i < tupleNum; i++) {
      tuple = new VTuple(3);
      tuple.put(0, DatumFactory.createInt(i + 1));
      tuple.put(1, DatumFactory.createLong(25l));
      tuple.put(2, stringDatum);
      appender.addTuple(tuple);
//      System.out.println(tuple.toString());
    }
    finalTuple = tuple;
    long end = System.currentTimeMillis();
    appender.close();

    TableStat stat = appender.getStats();
    assertEquals(tupleNum, stat.getNumRows().longValue());

    System.out.println("append time: " + (end-start));

    FileStatus file = fs.getFileStatus(dataPath);
    TableProto proto = (TableProto) FileUtil.loadProto(conf, metaPath,
        TableProto.getDefaultInstance());
    meta = new TableMetaImpl(proto);
    Fragment fragment = new Fragment("test.tbl", dataPath, meta, 0, file.getLen());
    int tupleCnt = 0;
    Scanner scanner = rowFile.openSingleScanner(schema, fragment);
    start = System.currentTimeMillis();
    while ((tuple=scanner.next()) != null) {
      tupleCnt++;
//      System.out.println(tuple.toString());
    }
    end = System.currentTimeMillis();

    assertEquals(tupleNum, tupleCnt);
    System.out.println("scan time: " + (end - start));

    Random random = new Random(System.currentTimeMillis());
    int fileStart = random.nextInt((int)file.getLen());
    fragment = new Fragment("test.tbl", dataPath, meta, fileStart, file.getLen());
    scanner = rowFile.openSingleScanner(schema, fragment);
    Tuple scannedTuple = null;
    while ((tuple=scanner.next()) != null) {
      scannedTuple = tuple;
      tupleCnt++;
    }
    assertEquals(finalTuple, scannedTuple);
  }
}
