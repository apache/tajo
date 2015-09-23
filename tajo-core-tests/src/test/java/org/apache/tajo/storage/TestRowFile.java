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

import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.TableProto;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.util.FileUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class TestRowFile {
  private static final Log LOG = LogFactory.getLog(TestRowFile.class);

  private TajoTestingCluster cluster;
  private TajoConf conf;

  @Before
  public void setup() throws Exception {
    cluster = TpchTestBase.getInstance().getTestingCluster();
    conf = cluster.getConfiguration();
  }

  @After
  public void teardown() throws Exception {
  }

  @Test
  public void test() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("age", Type.INT8);
    schema.addColumn("description", Type.TEXT);

    TableMeta meta = CatalogUtil.newTableMeta("ROWFILE");

    FileTablespace sm = (FileTablespace) TablespaceManager.get(cluster.getDefaultFileSystem().getUri());

    Path tablePath = new Path("/test");
    Path dataPath = new Path(tablePath, "test.tbl");
    FileSystem fs = sm.getFileSystem();
    fs.mkdirs(tablePath);

    Appender appender = sm.getAppender(meta, schema, dataPath);
    appender.enableStats();
    appender.init();

    int tupleNum = 200;
    Tuple tuple;
    Datum stringDatum = DatumFactory.createText("abcdefghijklmnopqrstuvwxyz");
    Set<Integer> idSet = Sets.newHashSet();

    tuple = new VTuple(3);
    long start = System.currentTimeMillis();
    for(int i = 0; i < tupleNum; i++) {
      tuple.put(0, DatumFactory.createInt4(i + 1));
      tuple.put(1, DatumFactory.createInt8(25l));
      tuple.put(2, stringDatum);
      appender.addTuple(tuple);
      idSet.add(i+1);
    }
    appender.close();

    TableStats stat = appender.getStats();
    assertEquals(tupleNum, stat.getNumRows().longValue());

    FileStatus file = fs.getFileStatus(dataPath);
    FileFragment fragment = new FileFragment("test.tbl", dataPath, 0, file.getLen());

    int tupleCnt = 0;
    start = System.currentTimeMillis();
    Scanner scanner = sm.getScanner(meta, schema, fragment, null);
    scanner.init();
    while ((tuple=scanner.next()) != null) {
      tupleCnt++;
    }
    scanner.close();

    assertEquals(tupleNum, tupleCnt);

    tupleCnt = 0;
    long fileStart = 0;
    long fileLen = file.getLen()/13;

    for (int i = 0; i < 13; i++) {
      fragment = new FileFragment("test.tbl", dataPath, fileStart, fileLen);
      scanner = new RowFile.RowFileScanner(conf, schema, meta, fragment);
      scanner.init();
      while ((tuple=scanner.next()) != null) {
        if (!idSet.remove(tuple.getInt4(0)) && LOG.isDebugEnabled()) {
          LOG.debug("duplicated! " + tuple.getInt4(0));
        }
        tupleCnt++;
      }
      scanner.close();
      fileStart += fileLen;
      if (i == 11) {
        fileLen = file.getLen() - fileStart;
      }
    }
    assertEquals(tupleNum, tupleCnt);
  }
}
