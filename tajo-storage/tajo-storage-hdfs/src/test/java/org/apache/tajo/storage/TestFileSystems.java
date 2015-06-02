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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.fragment.Fragment;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestFileSystems {

  private static String TEST_PATH = "target/test-data/TestFileSystem";
  private TajoConf conf;
  private FileTablespace sm;
  private FileSystem fs;
  private Path testDir;

  public TestFileSystems(FileSystem fs) throws IOException {
    this.fs = fs;
    this.conf = new TajoConf(fs.getConf());
    sm = (FileTablespace) TableSpaceManager.getFileStorageManager(conf);
    testDir = getTestDir(this.fs, TEST_PATH);
  }

  public Path getTestDir(FileSystem fs, String dir) throws IOException {
    Path path = new Path(dir);
    if(fs.exists(path))
      fs.delete(path, true);

    fs.mkdirs(path);

    return fs.makeQualified(path);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> generateParameters() throws IOException {
    return Arrays.asList(new Object[][]{
        {FileSystem.getLocal(new TajoConf())},
    });
  }

  @Before
  public void setup() throws IOException {
    if (!(fs instanceof LocalFileSystem)) {
      conf.set("fs.local.block.size", "10");
      fs.initialize(URI.create(fs.getScheme() + ":///"), conf);
      fs.setConf(conf);
    }
  }

  @After
  public void tearDown() throws IOException {
    if (!(fs instanceof LocalFileSystem)) {
      fs.setConf(new TajoConf());
    }
  }

  @Test
  public void testBlockSplit() throws IOException {

    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("age", Type.INT4);
    schema.addColumn("name", Type.TEXT);

    TableMeta meta = CatalogUtil.newTableMeta("CSV");

    Tuple[] tuples = new Tuple[4];
    for (int i = 0; i < tuples.length; i++) {
      tuples[i] = new VTuple(new Datum[]{
          DatumFactory.createInt4(i),
          DatumFactory.createInt4(i + 32),
          DatumFactory.createText("name" + i)});
    }

    Path path = StorageUtil.concatPath(testDir, "testGetScannerAndAppender",
        "table.csv");
    fs.mkdirs(path.getParent());

    Appender appender = sm.getAppender(meta, schema, path);
    appender.init();
    for (Tuple t : tuples) {
      appender.addTuple(t);
    }
    appender.close();
    FileStatus fileStatus = fs.getFileStatus(path);

    List<Fragment> splits = sm.getSplits("table", meta, schema, path);
    int splitSize = (int) Math.ceil(fileStatus.getLen() / (double) fileStatus.getBlockSize());
    assertEquals(splitSize, splits.size());

    for (Fragment fragment : splits) {
      assertTrue(fragment.getLength() <= fileStatus.getBlockSize());
    }
  }
}