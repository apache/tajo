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

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tajo.TajoTestingCluster;
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
import tajo.util.FileUtil;

import java.io.IOException;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class TestRowFile {
  private TajoTestingCluster util;
  private Configuration conf;
  private RowFile rowFile;

  @Before
  public void setup() throws Exception {
    util = new TajoTestingCluster();
    conf = util.getConfiguration();
    conf.setInt(ConfVars.RAWFILE_SYNC_INTERVAL.varname, 100);
    util.startMiniDFSCluster(1);
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
    schema.addColumn("description", DataType.STRING2);

    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.ROWFILE);

    Path tablePath = new Path("hdfs:///test");
    Path metaPath = new Path(tablePath, ".meta");
    Path dataDir = new Path(tablePath, "data");
    Path dataPath = new Path(dataDir, "test.tbl");
    FileSystem fs = tablePath.getFileSystem(conf);
    fs.mkdirs(tablePath);
    fs.mkdirs(dataDir);

    FileUtil.writeProto(util.getDefaultFileSystem(), metaPath, meta.getProto());

    Appender appender = rowFile.getAppender(meta, dataPath);

    int tupleNum = 100000;
    Tuple tuple;
    Datum stringDatum = DatumFactory.createString2("abcdefghijklmnopqrstuvwxyz");
    Set<Integer> idSet = Sets.newHashSet();

    tuple = new VTuple(3);
    long start = System.currentTimeMillis();
    for(int i = 0; i < tupleNum; i++) {
      tuple.put(0, DatumFactory.createInt(i + 1));
      tuple.put(1, DatumFactory.createLong(25l));
      tuple.put(2, stringDatum);
      appender.addTuple(tuple);
      idSet.add(i+1);
//      System.out.println(tuple.toString());
    }

    long end = System.currentTimeMillis();
    appender.close();

    TableStat stat = appender.getStats();
    assertEquals(tupleNum, stat.getNumRows().longValue());

    System.out.println("append time: " + (end-start));

    FileStatus file = fs.getFileStatus(dataPath);
    TableProto proto = (TableProto) FileUtil.loadProto(
        util.getDefaultFileSystem(), metaPath, TableProto.getDefaultInstance());
    meta = new TableMetaImpl(proto);
    Fragment fragment = new Fragment("test.tbl", dataPath, meta, 0, file.getLen(), null);

    int tupleCnt = 0;
    start = System.currentTimeMillis();
    Scanner scanner = rowFile.openSingleScanner(schema, fragment);
    while ((tuple=scanner.next()) != null) {
      tupleCnt++;
//      System.out.println(tuple.toString());
    }
    scanner.close();
    end = System.currentTimeMillis();

    assertEquals(tupleNum, tupleCnt);
    System.out.println("scan time: " + (end - start));

    tupleCnt = 0;
    long fileStart = 0;
    long fileLen = file.getLen()/13;
    System.out.println("total length: " + file.getLen());

    for (int i = 0; i < 13; i++) {
      System.out.println("range: " + fileStart + ", " + fileLen);
      fragment = new Fragment("test.tbl", dataPath, meta, fileStart, fileLen, null);
      scanner = rowFile.openSingleScanner(schema, fragment);
      while ((tuple=scanner.next()) != null) {
        if (!idSet.remove(tuple.get(0).asInt())) {
          System.out.println("duplicated! " + tuple.get(0).asInt());
        }
        tupleCnt++;
      }
      System.out.println("tuple count: " + tupleCnt);
      scanner.close();
      fileStart += fileLen;
      if (i == 11) {
        fileLen = file.getLen() - fileStart;
      }
    }

    for (Integer id : idSet) {
      System.out.println("remaining id: " + id);
    }
    assertEquals(tupleNum, tupleCnt);
  }
}
