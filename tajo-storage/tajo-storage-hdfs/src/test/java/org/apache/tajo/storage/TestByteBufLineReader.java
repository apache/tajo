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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.text.ByteBufLineReader;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.Charset;
import java.util.UUID;

import static org.junit.Assert.*;

public class TestByteBufLineReader {
  private TajoConf conf;
  private static String TEST_PATH = "target/test-data/TestByteBufLineReader";
  private Path testDir;
  private FileSystem fs;
  private static String LINE = "A big data warehouse system on Hadoop";

  @Before
  public void setUp() throws Exception {
    conf = new TajoConf();
    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    fs = testDir.getFileSystem(conf);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testReaderWithLocalFS() throws Exception {
    Path tablePath = new Path(testDir, "testReaderWithLocalFS");
    Path filePath = new Path(tablePath, "data.dat");

    FileSystem fileSystem = filePath.getFileSystem(conf);
    assertTrue(fileSystem instanceof LocalFileSystem);

    FSDataOutputStream out = fs.create(filePath, true);
    out.write(LINE.getBytes(Charset.defaultCharset()));
    out.write('\n');
    out.close();

    assertTrue(fs.exists(filePath));

    FSDataInputStream inputStream = fs.open(filePath);
    assertFalse(inputStream.getWrappedStream() instanceof ByteBufferReadable);

    ByteBufLineReader lineReader = new ByteBufLineReader(new FSDataInputChannel(inputStream));
    assertEquals(LINE, lineReader.readLine());
    lineReader.seek(0);
    assertEquals(LINE, lineReader.readLine());
    assertNull(lineReader.readLine());

    lineReader.close();
    fs.close();
  }

  @Test(timeout = 60000)
  public void testReaderWithDFS() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    String testDataPath = TEST_PATH + "/" + UUID.randomUUID().toString();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, testDataPath);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);
    conf.setBoolean(DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED, true);

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2).waitSafeMode(true).build();

    TajoConf tajoConf = new TajoConf(conf);
    tajoConf.setVar(TajoConf.ConfVars.ROOT_DIR, cluster.getFileSystem().getUri() + "/tajo");

    Path tablePath = new Path("/testReaderWithDFS");
    Path filePath = new Path(tablePath, "data.dat");
    try {
      DistributedFileSystem fs = cluster.getFileSystem();
      FSDataOutputStream out = fs.create(filePath, true);
      out.write(LINE.getBytes(Charset.defaultCharset()));
      out.write('\n');
      out.close();

      assertTrue(fs.exists(filePath));
      FSDataInputStream inputStream = fs.open(filePath);
      assertTrue(inputStream.getWrappedStream() instanceof ByteBufferReadable);

      ByteBufLineReader lineReader = new ByteBufLineReader(new FSDataInputChannel(inputStream));
      assertEquals(LINE, lineReader.readLine());
      lineReader.seek(0);
      assertEquals(LINE, lineReader.readLine());
      assertNull(lineReader.readLine());

      lineReader.close();
      fs.close();
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testReaderWithNIO() throws Exception {
    Path tablePath = new Path(testDir, "testReaderWithNIO");
    Path filePath = new Path(tablePath, "data.dat");

    FileSystem fileSystem = filePath.getFileSystem(conf);
    assertTrue(fileSystem instanceof LocalFileSystem);

    FSDataOutputStream out = fs.create(filePath, true);
    out.write(LINE.getBytes(Charset.defaultCharset()));
    out.write('\n');
    out.close();

    File file = new File(filePath.toUri());
    assertTrue(file.exists());

    FileInputStream inputStream = new FileInputStream(file);
    ByteBufLineReader lineReader = new ByteBufLineReader(new LocalFileInputChannel(inputStream));

    assertEquals(LINE, lineReader.readLine());
    lineReader.seek(0);
    assertEquals(LINE, lineReader.readLine());
    assertNull(lineReader.readLine());

    lineReader.close();
    inputStream.close();
  }
}
