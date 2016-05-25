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

import com.google.common.collect.Lists;
import net.minidev.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaBuilder;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;

public class TestFileTablespace {
	private TajoConf conf;
	private static String TEST_PATH = "target/test-data/hdfs";
  private Path testDir;
  private FileSystem localFs;

	@Before
	public void setUp() throws Exception {
		conf = new TajoConf();
    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    localFs = testDir.getFileSystem(conf);
	}

	@After
	public void tearDown() throws Exception {
	}

  public static HdfsConfiguration getTestHdfsConfiguration() {
    HdfsConfiguration conf = new HdfsConfiguration();
    String testDataPath = new File(TEST_PATH + "/" + UUID.randomUUID().toString()).getAbsolutePath();

    String namenodeDir = new File(testDataPath, "name").getAbsolutePath();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, testDataPath);
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, namenodeDir);
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, namenodeDir);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_KEY, 0);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);

    return conf;
  }

  @Test
	public final void testGetScannerAndAppender() throws IOException {
    Schema schema = SchemaBuilder.builder()
        .add("id", Type.INT4)
        .add("age", Type.INT4)
        .add("name", Type.TEXT)
        .build();

		TableMeta meta = CatalogUtil.newTableMeta(BuiltinStorages.TEXT, conf);
		
		VTuple[] tuples = new VTuple[4];
		for(int i=0; i < tuples.length; i++) {
		  tuples[i] = new VTuple(new Datum[] {
          DatumFactory.createInt4(i),
		      DatumFactory.createInt4(i + 32),
		      DatumFactory.createText("name" + i)});
		}

    Path path = StorageUtil.concatPath(testDir, "testGetScannerAndAppender", "table.csv");
    localFs.mkdirs(path.getParent());
    FileTablespace fileStorageManager = TablespaceManager.getLocalFs();
    assertEquals(localFs.getUri(), fileStorageManager.getFileSystem().getUri());

		Appender appender = fileStorageManager.getAppender(meta, schema, path);
    appender.init();
		for(Tuple t : tuples) {
		  appender.addTuple(t);
		}
		appender.close();

		Scanner scanner = fileStorageManager.getFileScanner(meta, schema, path);
    scanner.init();
		int i=0;
		while(scanner.next() != null) {
			i++;
		}
		assertEquals(4,i);
    localFs.delete(path, true);
	}

  @Test(timeout = 120000)
  public void testGetSplit() throws Exception {
    final Configuration hdfsConf = getTestHdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(hdfsConf)
        .numDataNodes(1).format(true).build();

    int testCount = 10;
    Path tablePath = new Path("/testGetSplit");
    try {
      DistributedFileSystem fs = cluster.getFileSystem();

      // Create test partitions
      List<Path> partitions = Lists.newArrayList();
      for (int i =0; i < testCount; i++){
        Path tmpFile = new Path(tablePath, String.valueOf(i));
        DFSTestUtil.createFile(fs, new Path(tmpFile, "tmpfile.dat"), 10, (short) 2, 0xDEADDEADl);
        partitions.add(tmpFile);
      }

      assertTrue(fs.exists(tablePath));
      FileTablespace space = new FileTablespace("testGetSplit", fs.getUri(), null);
      space.init(conf);

      TablespaceManager.addTableSpaceForTest(space);
      assertEquals(fs.getUri(), space.getUri());

      Schema schema = SchemaBuilder.builder()
          .add("id", Type.INT4)
          .add("age", Type.INT4)
          .add("name", Type.TEXT)
          .build();

      TableMeta meta = CatalogUtil.newTableMeta(BuiltinStorages.TEXT, conf);

      List<Fragment> splits = Lists.newArrayList();
      // Get FileFragments in partition batch
      splits.addAll(space.getSplits("data", meta, schema, false, partitions.toArray(new Path[partitions.size()])));
      assertEquals(testCount, splits.size());
      // -1 is unknown volumeId
      assertEquals(DataLocation.UNKNOWN_VOLUME_ID, ((FileFragment)splits.get(0)).getDiskIds()[0].intValue());

      splits.clear();
      splits.addAll(space.getSplits("data", meta, schema, false,
          partitions.subList(0, partitions.size() / 2).toArray(new Path[partitions.size() / 2])));
      assertEquals(testCount / 2, splits.size());
      assertEquals(1, splits.get(0).getHostNames().size());
      assertEquals(DataLocation.UNKNOWN_VOLUME_ID, ((FileFragment)splits.get(0)).getDiskIds()[0].intValue());

      fs.close();
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout = 120000)
  public void testZeroLengthSplit() throws Exception {
    final Configuration hdfsConf = getTestHdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(hdfsConf)
        .numDataNodes(1).format(true).build();

    int testCount = 10;
    Path tablePath = new Path("/testZeroLengthSplit");
    try {
      DistributedFileSystem fs = cluster.getFileSystem();

      // Create test partitions
      List<Path> partitions = Lists.newArrayList();
      for (int i =0; i < testCount; i++){
        Path tmpFile = new Path(tablePath, String.valueOf(i));

        //creates zero length file
        DFSTestUtil.createFile(fs, new Path(tmpFile, "tmpfile.dat"), 0, (short) 2, 0xDEADDEADl);
        partitions.add(tmpFile);
      }

      assertTrue(fs.exists(tablePath));

      FileTablespace space = new FileTablespace("testZeroLengthSplit", fs.getUri(), new JSONObject());
      space.init(conf);
      TablespaceManager.addTableSpaceForTest(space);
      assertEquals(fs.getUri(), space.getUri());

      Schema schema = SchemaBuilder.builder()
          .add("id", Type.INT4)
          .add("age", Type.INT4)
          .add("name", Type.TEXT)
          .build();

      TableMeta meta = CatalogUtil.newTableMeta(BuiltinStorages.TEXT, conf);

      List<Fragment> splits = Lists.newArrayList();
      // Get FileFragments in partition batch
      splits.addAll(space.getSplits("data", meta, schema, false, partitions.toArray(new Path[partitions.size()])));
      assertEquals(0, splits.size());

      fs.close();
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout = 120000)
  public void testGetSplitWithBlockStorageLocationsBatching() throws Exception {
    final Configuration hdfsConf = getTestHdfsConfiguration();

    hdfsConf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 2);
    hdfsConf.setBoolean(DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED, true);
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(hdfsConf)
        .numDataNodes(2).format(true).build();

    int testCount = 10;
    Path tablePath = new Path("/testGetSplitWithBlockStorageLocationsBatching");
    try {
      DistributedFileSystem fs = cluster.getFileSystem();

      // Create test files
      for (int i = 0; i < testCount; i++) {
        Path tmpFile = new Path(tablePath, "tmpfile" + i + ".dat");
        DFSTestUtil.createFile(fs, tmpFile, 10, (short) 2, 0xDEADDEADl);
      }
      assertTrue(fs.exists(tablePath));

      FileTablespace sm = new FileTablespace("testGetSplitWithBlockStorageLocationsBatching", fs.getUri(), null);
      sm.init(new TajoConf(hdfsConf));
      TablespaceManager.addTableSpaceForTest(sm);
      assertEquals(fs.getUri(), sm.getUri());

      Schema schema = SchemaBuilder.builder()
          .add("id", Type.INT4)
          .add("age", Type.INT4)
          .add("name", Type.TEXT)
          .build();

      TableMeta meta = CatalogUtil.newTableMeta(BuiltinStorages.TEXT, conf);

      List<Fragment> splits = Lists.newArrayList();
      splits.addAll(sm.getSplits("data", meta, schema, false, tablePath));

      assertEquals(testCount, splits.size());
      assertEquals(2, splits.get(0).getHostNames().size());
      assertEquals(2, ((FileFragment)splits.get(0)).getDiskIds().length);
      assertNotEquals(DataLocation.UNKNOWN_VOLUME_ID, ((FileFragment)splits.get(0)).getDiskIds()[0].intValue());

      fs.close();
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout = 120000)
  public void testGetFileTablespace() throws Exception {
    final Configuration hdfsConf = getTestHdfsConfiguration();

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(hdfsConf)
            .numDataNodes(1).format(true).build();
    URI uri = URI.create(cluster.getFileSystem().getUri() + "/tajo");

    try {
      /* Local FileSystem */
      FileTablespace space = TablespaceManager.getLocalFs();
      assertEquals(localFs.getUri(), space.getFileSystem().getUri());

      FileTablespace distTablespace = new FileTablespace("testGetFileTablespace", uri, null);
      distTablespace.init(conf);

      TablespaceManager.addTableSpaceForTest(distTablespace);

      /* Distributed FileSystem */
      space = TablespaceManager.get(uri);
      assertEquals(cluster.getFileSystem().getUri(), space.getFileSystem().getUri());

      space = TablespaceManager.getByName("testGetFileTablespace");
      assertEquals(cluster.getFileSystem().getUri(), space.getFileSystem().getUri());
    } finally {
      cluster.shutdown();
    }
  }
}
