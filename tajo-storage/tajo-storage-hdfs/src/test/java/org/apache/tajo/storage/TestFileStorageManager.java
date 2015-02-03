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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
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

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;

public class TestFileStorageManager {
	private TajoConf conf;
	private static String TEST_PATH = "target/test-data/TestFileStorageManager";
  private Path testDir;
  private FileSystem fs;

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
	public final void testGetScannerAndAppender() throws IOException {
		Schema schema = new Schema();
		schema.addColumn("id", Type.INT4);
		schema.addColumn("age",Type.INT4);
		schema.addColumn("name",Type.TEXT);

		TableMeta meta = CatalogUtil.newTableMeta(StoreType.CSV);
		
		Tuple[] tuples = new Tuple[4];
		for(int i=0; i < tuples.length; i++) {
		  tuples[i] = new VTuple(3);
		  tuples[i].put(new Datum[] {
          DatumFactory.createInt4(i),
		      DatumFactory.createInt4(i + 32),
		      DatumFactory.createText("name" + i)});
		}

    Path path = StorageUtil.concatPath(testDir, "testGetScannerAndAppender", "table.csv");
    fs.mkdirs(path.getParent());
    FileStorageManager fileStorageManager = (FileStorageManager)StorageManager.getFileStorageManager(conf);
    assertEquals(fs.getUri(), fileStorageManager.getFileSystem().getUri());

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
	}

  @Test
  public void testGetSplit() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    String testDataPath = TEST_PATH + "/" + UUID.randomUUID().toString();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, testDataPath);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);
    conf.setBoolean(DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED, false);

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1).build();
    cluster.waitClusterUp();
    TajoConf tajoConf = new TajoConf(conf);
    tajoConf.setVar(TajoConf.ConfVars.ROOT_DIR, cluster.getFileSystem().getUri() + "/tajo");

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
      FileStorageManager sm = (FileStorageManager)StorageManager.getFileStorageManager(tajoConf);
      assertEquals(fs.getUri(), sm.getFileSystem().getUri());

      Schema schema = new Schema();
      schema.addColumn("id", Type.INT4);
      schema.addColumn("age",Type.INT4);
      schema.addColumn("name",Type.TEXT);
      TableMeta meta = CatalogUtil.newTableMeta(StoreType.CSV);

      List<Fragment> splits = Lists.newArrayList();
      // Get FileFragments in partition batch
      splits.addAll(sm.getSplits("data", meta, schema, partitions.toArray(new Path[partitions.size()])));
      assertEquals(testCount, splits.size());
      // -1 is unknown volumeId
      assertEquals(-1, ((FileFragment)splits.get(0)).getDiskIds()[0]);

      splits.clear();
      splits.addAll(sm.getSplits("data", meta, schema,
          partitions.subList(0, partitions.size() / 2).toArray(new Path[partitions.size() / 2])));
      assertEquals(testCount / 2, splits.size());
      assertEquals(1, splits.get(0).getHosts().length);
      assertEquals(-1, ((FileFragment)splits.get(0)).getDiskIds()[0]);
      fs.close();
    } finally {
      cluster.shutdown(true);
    }
  }

  @Test
  public void testGetSplitWithBlockStorageLocationsBatching() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    String testDataPath = TEST_PATH + "/" + UUID.randomUUID().toString();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, testDataPath);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);
    conf.setBoolean(DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED, true);

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2).build();
    cluster.waitClusterUp();

    TajoConf tajoConf = new TajoConf(conf);
    tajoConf.setVar(TajoConf.ConfVars.ROOT_DIR, cluster.getFileSystem().getUri() + "/tajo");

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
      FileStorageManager sm = (FileStorageManager)StorageManager.getFileStorageManager(tajoConf);
      assertEquals(fs.getUri(), sm.getFileSystem().getUri());

      Schema schema = new Schema();
      schema.addColumn("id", Type.INT4);
      schema.addColumn("age", Type.INT4);
      schema.addColumn("name", Type.TEXT);
      TableMeta meta = CatalogUtil.newTableMeta(StoreType.CSV);

      List<Fragment> splits = Lists.newArrayList();
      splits.addAll(sm.getSplits("data", meta, schema, tablePath));

      assertEquals(testCount, splits.size());
      assertEquals(2, splits.get(0).getHosts().length);
      assertEquals(2, ((FileFragment)splits.get(0)).getDiskIds().length);
      assertNotEquals(-1, ((FileFragment)splits.get(0)).getDiskIds()[0]);
      fs.close();
    } finally {
      cluster.shutdown(true);
    }
  }

  @Test
  public void testStoreType() throws Exception {
    final Configuration hdfsConf = new HdfsConfiguration();
    String testDataPath = TEST_PATH + "/" + UUID.randomUUID().toString();
    hdfsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, testDataPath);
    hdfsConf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);
    hdfsConf.setBoolean(DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED, true);

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(hdfsConf)
        .numDataNodes(2).build();
    cluster.waitClusterUp();

    TajoConf tajoConf = new TajoConf(hdfsConf);
    tajoConf.setVar(TajoConf.ConfVars.ROOT_DIR, cluster.getFileSystem().getUri() + "/tajo");

    try {
      /* Local FileSystem */
      FileStorageManager sm = (FileStorageManager)StorageManager.getStorageManager(conf, StoreType.CSV);
      assertEquals(fs.getUri(), sm.getFileSystem().getUri());

      /* Distributed FileSystem */
      sm = (FileStorageManager)StorageManager.getStorageManager(tajoConf, StoreType.CSV);
      assertNotEquals(fs.getUri(), sm.getFileSystem().getUri());
      assertEquals(cluster.getFileSystem().getUri(), sm.getFileSystem().getUri());
    } finally {
      cluster.shutdown(true);
    }
  }
}
