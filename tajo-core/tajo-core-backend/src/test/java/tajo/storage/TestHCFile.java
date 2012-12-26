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

import com.google.common.collect.Lists;
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
import tajo.catalog.proto.CatalogProtos.CompressType;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.conf.TajoConf;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.storage.exception.UnknownCodecException;
import tajo.storage.exception.UnknownDataTypeException;
import tajo.storage.hcfile.ColumnMeta;
import tajo.storage.hcfile.HCFile.Appender;
import tajo.storage.hcfile.HCFile.Scanner;
import tajo.storage.hcfile.HCTupleAppender;
import tajo.storage.hcfile.HColumnMetaWritable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class TestHCFile {

  private static TajoTestingCluster util;
  private static TajoConf conf;
  private static Random random;

  @Before
  public void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.startMiniDFSCluster(1);
    conf = util.getConfiguration();
    conf.setInt("dfs.blocksize", 65535);
    random = new Random(System.currentTimeMillis());
  }

  @After
  public void tearDown() throws Exception {
    util.shutdownMiniDFSCluster();
  }

  @Test
  public void testInt()
      throws UnknownCodecException, IOException, UnknownDataTypeException {
    int rowNum = 1000;
    Path path = new Path("hdfs:///hcfile.int");
    List<Integer> data = Lists.newArrayList();

    ColumnMeta
        meta = new HColumnMetaWritable(0, DataType.INT, CompressType.COMP_NONE,
        false, false, true);
    long before = System.currentTimeMillis();
    Appender appender = new Appender(conf, meta, path);

    for (int i = 0; i < rowNum; i++) {
      data.add(i);
      appender.append(DatumFactory.createInt(data.get(data.size()-1)));
    }
    appender.close();
    long after = System.currentTimeMillis();
    System.out.println("write time: " + (after-before));

    before = System.currentTimeMillis();
    Scanner scanner = new Scanner(conf, path);

    for (Integer i : data) {
      assertEquals(i.intValue(), scanner.get().asInt());
    }
    after = System.currentTimeMillis();
    System.out.println("sequential read time: " + (after-before));
    scanner.close();

    before = System.currentTimeMillis();
    scanner = new Scanner(conf, path);
    scanner.first();
    assertEquals(data.get(0).intValue(), scanner.get().asInt());
    after = System.currentTimeMillis();
    System.out.println("after first() read time: " + (after-before));
    scanner.close();

    before = System.currentTimeMillis();
    scanner = new Scanner(conf, path);
    scanner.last();
    assertEquals(data.get(data.size()-1).intValue(), scanner.get().asInt());
    after = System.currentTimeMillis();
    System.out.println("after last() read time: " + (after-before));
    scanner.close();

    before = System.currentTimeMillis();
    scanner = new Scanner(conf, path);
    int randomIndex = random.nextInt(rowNum);
    scanner.pos(randomIndex);
    assertEquals(data.get(randomIndex).intValue(), scanner.get().asInt());
    after = System.currentTimeMillis();
    System.out.println("after pos() read time: " + (after-before));
    scanner.close();
  }

  @Test
  public void testString()
      throws IOException, UnknownCodecException, UnknownDataTypeException {
    int rowNum = 1000;
    Path path = new Path("hdfs:///hcfile.string");
    List<String> data = Lists.newArrayList();

    ColumnMeta meta = new HColumnMetaWritable(0, DataType.STRING, CompressType.COMP_NONE,
        false, false, true);
    Appender appender = new Appender(conf, meta, path);

    String randomStr;
    for (int i = 0; i < rowNum; i++) {
      randomStr = getRandomString(10);
      data.add(randomStr);
      appender.append(DatumFactory.createString(randomStr));
    }
    appender.close();

    Scanner scanner = new Scanner(conf, path);
    for (String s : data) {
      assertEquals(s, scanner.get().asChars());
    }
    scanner.close();
  }

  @Test
  public void testHCTupleAppender()
      throws UnknownCodecException, IOException, UnknownDataTypeException {
    int tupleNum = 1000;

    Path tablePath = new Path("hdfs:///table");
    Schema schema = new Schema();
    schema.addColumn("id", DataType.INT);
    schema.addColumn("age", DataType.LONG);
    schema.addColumn("description", DataType.STRING);
    schema.addColumn("char", DataType.CHAR);
    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.HCFILE);

    HCTupleAppender appender = new HCTupleAppender(conf, meta, 2, tablePath);
    Datum stringDatum = DatumFactory.createString("abcdefghijklmnopqrstuvwxyz");

    int i;
    Tuple tuple = new VTuple(4);
    for(i = 0; i < tupleNum; i++) {
      tuple.put(0, DatumFactory.createInt(i));
      tuple.put(1, DatumFactory.createLong(25l));
      tuple.put(2, stringDatum);
      tuple.put(3, DatumFactory.createChar('a'));
      appender.addTuple(tuple);
    }
    appender.close();

    FileSystem fs = tablePath.getFileSystem(conf);
    FileStatus[] files = fs.listStatus(new Path(tablePath, "data"));
    Path[] shardDirs = new Path[files.length];
    for (i = 0; i < files.length; i++) {
      shardDirs[i] = files[i].getPath();
    }
    Arrays.sort(shardDirs, new NumericPathComparator());

    Scanner scanner;
    Datum datum;
    int cnt = 0;

    for (i = 0; i < shardDirs.length; i++) {
      scanner = new Scanner(conf, new Path(shardDirs[i], "id_0"));
      while ((datum=scanner.get()) != null) {
        assertEquals(cnt++, datum.asInt());
      }
      scanner.close();

      scanner = new Scanner(conf, new Path(shardDirs[i], "age_0"));
      while ((datum=scanner.get()) != null) {
        assertEquals(25l, datum.asLong());
      }
      scanner.close();

      scanner = new Scanner(conf, new Path(shardDirs[i], "description_0"));
      while ((datum=scanner.get()) != null) {
        assertEquals("abcdefghijklmnopqrstuvwxyz", datum.asChars());
      }
      scanner.close();

      scanner = new Scanner(conf, new Path(shardDirs[i], "char_0"));
      while ((datum=scanner.get()) != null) {
        assertEquals('a', datum.asChar());
      }
      scanner.close();
    }
  }

//  @Test
//  public void testOrders()
//      throws IOException, UnknownCodecException, UnknownDataTypeException {
//    Path tablePath = new Path("file:///home/jihoon/work/develop/tpch/customer");
//    Path metaPath = new Path(tablePath, ".meta");
//    Path dataDir = new Path(tablePath, "data");
//    Path outPath = new Path("file:///home/jihoon/work/develop/ColumnLoader/target/test-data/customer");
//    FileSystem fs = metaPath.getFileSystem(conf);
//
//    FSDataInputStream in = fs.open(metaPath);
//    TableProto proto = (TableProto) FileUtil.loadProto(in, TableProto.getDefaultInstance());
//    TableMeta meta = new TableMetaImpl(proto);
//    in.close();
//
//    Tuple tuple;
//    Fragment fragment;
//    CSVFile.CSVScanner scanner;
//    HCTupleAppender appender = new HCTupleAppender(conf, meta, meta.getSchema().getColumn(0), outPath);
//
//    for (FileStatus file : fs.listStatus(dataDir)) {
//      if (file.getPath().getName().equals(".index")) {
//        continue;
//      }
//      fragment = new Fragment("0", file.getPath(), meta, 0, file.getLen());
//      scanner = new CSVScanner(conf, meta.getSchema(), fragment);
//      while ((tuple=scanner.next()) != null) {
//        appender.addTuple(tuple);
//      }
//      scanner.close();
//    }
//    appender.close();
//
//  }

  private static String getRandomString(int length) {
    StringBuffer buffer = new StringBuffer();

    String chars[] =
        "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z".split(",");

    for (int i=0 ; i<length ; i++)
    {
      buffer.append(chars[random.nextInt(chars.length)]);
    }
    return buffer.toString();
  }
}
