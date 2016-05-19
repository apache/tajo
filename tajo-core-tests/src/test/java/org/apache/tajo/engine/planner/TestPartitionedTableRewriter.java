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

package org.apache.tajo.engine.planner;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.*;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.parser.sql.SQLAnalyzer;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.partition.PartitionPruningHandle;
import org.apache.tajo.plan.rewrite.rules.PartitionedTableRewriter;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.FileUtil;
import org.junit.*;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.*;

public class TestPartitionedTableRewriter  {
  private TajoConf conf;
  private final String TEST_PATH = TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/TestPartitionedTableRewriter";
  private TajoTestingCluster util;
  private CatalogService catalog;
  private SQLAnalyzer analyzer;
  private LogicalPlanner planner;
  private Path testDir;
  private FileSystem fs;

  final static String PARTITION_TABLE_NAME = "tb_partition";
  final static String MULTIPLE_PARTITION_TABLE_NAME = "tb_multiple_partition";

  @Before
  public void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.initTestDir();
    util.startCatalogCluster();
    catalog = util.getCatalogService();
    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, testDir.toUri().toString());
    catalog.createDatabase(DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
    conf = util.getConfiguration();
    fs = FileSystem.get(conf);

    Schema schema = SchemaBuilder.builder()
      .add("n_nationkey", TajoDataTypes.Type.INT8)
      .add("n_name", TajoDataTypes.Type.TEXT)
      .add("n_regionkey", TajoDataTypes.Type.INT8)
      .build();

    TableMeta meta = CatalogUtil.newTableMeta(BuiltinStorages.TEXT, util.getConfiguration());

    createTableWithOnePartitionKeyColumn(fs, schema, meta);
    createTableWithMultiplePartitionKeyColumns(fs, schema, meta);

    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog, TablespaceManager.getInstance());
  }

  private void createTableWithOnePartitionKeyColumn(FileSystem fs, Schema schema,
                                                                       TableMeta meta) throws Exception {
    Schema partSchema = SchemaBuilder.builder()
      .add("key", TajoDataTypes.Type.TEXT)
      .build();

    PartitionMethodDesc partitionMethodDesc =
      new PartitionMethodDesc(DEFAULT_DATABASE_NAME, PARTITION_TABLE_NAME,
        CatalogProtos.PartitionType.COLUMN, "key", partSchema);

    Path tablePath = new Path(testDir, PARTITION_TABLE_NAME);
    fs.mkdirs(tablePath);

    TableDesc desc = CatalogUtil.newTableDesc(DEFAULT_DATABASE_NAME + "." + PARTITION_TABLE_NAME, schema, meta,
      tablePath, partitionMethodDesc);
    catalog.createTable(desc);

    TableDesc tableDesc = catalog.getTableDesc(DEFAULT_DATABASE_NAME + "." + PARTITION_TABLE_NAME);
    assertNotNull(tableDesc);

    Path path = new Path(tableDesc.getUri().toString() + "/key=part123");
    fs.mkdirs(path);
    FileUtil.writeTextToFile("1|ARGENTINA|1", new Path(path, "data"));

    path = new Path(tableDesc.getUri().toString() + "/key=part456");
    fs.mkdirs(path);
    FileUtil.writeTextToFile("2|BRAZIL|1", new Path(path, "data"));

    path = new Path(tableDesc.getUri().toString() + "/key=part789");
    fs.mkdirs(path);
    FileUtil.writeTextToFile("3|CANADA|1", new Path(path, "data"));
  }

  private void createTableWithMultiplePartitionKeyColumns(FileSystem fs,
    Schema schema, TableMeta meta) throws Exception {
    Schema partSchema = SchemaBuilder.builder()
      .add("key1", TajoDataTypes.Type.TEXT)
      .add("key2", TajoDataTypes.Type.TEXT)
      .add("key3", TajoDataTypes.Type.INT8)
      .build();

    PartitionMethodDesc partitionMethodDesc =
      new PartitionMethodDesc("default", MULTIPLE_PARTITION_TABLE_NAME,
        CatalogProtos.PartitionType.COLUMN, "key1,key2,key3", partSchema);

    Path tablePath = new Path(testDir, MULTIPLE_PARTITION_TABLE_NAME);
    fs.mkdirs(tablePath);

    TableDesc desc = CatalogUtil.newTableDesc(DEFAULT_DATABASE_NAME + "." + MULTIPLE_PARTITION_TABLE_NAME, schema,
      meta, tablePath, partitionMethodDesc);
    catalog.createTable(desc);

    TableDesc tableDesc = catalog.getTableDesc(DEFAULT_DATABASE_NAME + "." + MULTIPLE_PARTITION_TABLE_NAME);
    assertNotNull(tableDesc);

    Path path = new Path(tableDesc.getUri().toString() + "/key1=part123");
    fs.mkdirs(path);
    path = new Path(tableDesc.getUri().toString() + "/key1=part123/key2=supp123");
    fs.mkdirs(path);
    path = new Path(tableDesc.getUri().toString() + "/key1=part123/key2=supp123/key3=1");
    fs.mkdirs(path);
    FileUtil.writeTextToFile("1|ARGENTINA|1", new Path(path, "data"));

    path = new Path(tableDesc.getUri().toString() + "/key1=part123/key2=supp123/key3=2");
    fs.mkdirs(path);
    FileUtil.writeTextToFile("2|BRAZIL|1", new Path(path, "data"));

    path = new Path(tableDesc.getUri().toString() + "/key1=part789");
    fs.mkdirs(path);
    path = new Path(tableDesc.getUri().toString() + "/key1=part789/key2=supp789");
    fs.mkdirs(path);
    path = new Path(tableDesc.getUri().toString() + "/key1=part789/key2=supp789/key3=3");
    fs.mkdirs(path);
    FileUtil.writeTextToFile("3|CANADA|1", new Path(path, "data"));
  }

  @After
  public void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  @Test
  public void testFilterIncludePartitionKeyColumn() throws Exception {
    Expr expr = analyzer.parse("SELECT * FROM " + PARTITION_TABLE_NAME + " WHERE key = 'part456' ORDER BY key");
    QueryContext defaultContext = LocalTajoTestingUtility.createDummyContext(util.getConfiguration());
    LogicalPlan newPlan = planner.createPlan(defaultContext, expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();

    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;

    ProjectionNode projNode = root.getChild();

    assertEquals(NodeType.SORT, projNode.getChild().getType());
    SortNode sortNode = projNode.getChild();

    assertEquals(NodeType.SELECTION, sortNode.getChild().getType());
    SelectionNode selNode = sortNode.getChild();
    assertTrue(selNode.hasQual());

    assertEquals(NodeType.SCAN, selNode.getChild().getType());
    ScanNode scanNode = selNode.getChild();
    scanNode.setQual(selNode.getQual());

    PartitionedTableRewriter rewriter = new PartitionedTableRewriter();
    rewriter.setCatalog(catalog);

    PartitionPruningHandle partitionPruningHandle = rewriter.getPartitionPruningHandle(conf, scanNode);
    assertNotNull(partitionPruningHandle);

    Path[] filteredPaths = partitionPruningHandle.getPartitionPaths();
    assertEquals(1, filteredPaths.length);
    assertEquals("key=part456", filteredPaths[0].getName());

    String[] partitionKeys = partitionPruningHandle.getPartitionKeys();
    assertEquals(1, partitionKeys.length);
    assertEquals("key=part456", partitionKeys[0]);

    assertEquals(10L, partitionPruningHandle.getTotalVolume());
  }

  @Test
  public void testWithoutAnyFilters() throws Exception {
    Expr expr = analyzer.parse("SELECT * FROM " + PARTITION_TABLE_NAME + " ORDER BY key");
    QueryContext defaultContext = LocalTajoTestingUtility.createDummyContext(util.getConfiguration());
    LogicalPlan newPlan = planner.createPlan(defaultContext, expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();

    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;

    ProjectionNode projNode = root.getChild();

    assertEquals(NodeType.SORT, projNode.getChild().getType());
    SortNode sortNode = projNode.getChild();

    assertEquals(NodeType.SCAN, sortNode.getChild().getType());
    ScanNode scanNode = sortNode.getChild();

    PartitionedTableRewriter rewriter = new PartitionedTableRewriter();
    rewriter.setCatalog(catalog);

    PartitionPruningHandle partitionPruningHandle = rewriter.getPartitionPruningHandle(conf, scanNode);
    assertNotNull(partitionPruningHandle);

    Stream<Path> partitionPathStream = Stream.of(partitionPruningHandle.getPartitionPaths())
      .sorted((path1, path2) -> path1.compareTo(path2));
    List<Path> partitionPathList = partitionPathStream.collect(Collectors.toList());
    assertEquals(3, partitionPathList.size());
    assertTrue(partitionPathList.get(0).toString().endsWith("key=part123"));
    assertTrue(partitionPathList.get(1).toString().endsWith("key=part456"));
    assertTrue(partitionPathList.get(2).toString().endsWith("key=part789"));

    Stream<String> partitionKeysStream = Stream.of(partitionPruningHandle.getPartitionKeys())
      .sorted((keys1, keys2) -> keys1.compareTo(keys2));
    List<String> partitionKeysList = partitionKeysStream.collect(Collectors.toList());
    assertEquals(3, partitionKeysList.size());
    assertEquals(partitionKeysList.get(0), "key=part123");
    assertEquals(partitionKeysList.get(1), "key=part456");
    assertEquals(partitionKeysList.get(2), "key=part789");

    assertEquals(33L, partitionPruningHandle.getTotalVolume());
  }

  @Test
  public void testFilterIncludeNonExistingPartitionValue() throws Exception {
    Expr expr = analyzer.parse("SELECT * FROM " + PARTITION_TABLE_NAME + " WHERE key = 'part123456789'");
    QueryContext defaultContext = LocalTajoTestingUtility.createDummyContext(util.getConfiguration());
    LogicalPlan newPlan = planner.createPlan(defaultContext, expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();

    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;

    ProjectionNode projNode = root.getChild();

    assertEquals(NodeType.SELECTION, projNode.getChild().getType());
    SelectionNode selNode = projNode.getChild();
    assertTrue(selNode.hasQual());

    assertEquals(NodeType.SCAN, selNode.getChild().getType());
    ScanNode scanNode = selNode.getChild();
    scanNode.setQual(selNode.getQual());

    PartitionedTableRewriter rewriter = new PartitionedTableRewriter();
    rewriter.setCatalog(catalog);

    PartitionPruningHandle partitionPruningHandle = rewriter.getPartitionPruningHandle(conf, scanNode);
    assertNotNull(partitionPruningHandle);

    assertEquals(0, partitionPruningHandle.getPartitionPaths().length);
    assertEquals(0, partitionPruningHandle.getPartitionKeys().length);

    assertEquals(0L, partitionPruningHandle.getTotalVolume());
  }

  @Test
  public void testFilterIncludeNonPartitionKeyColumn() throws Exception {
    Expr expr = analyzer.parse("SELECT * FROM " + PARTITION_TABLE_NAME + " WHERE n_nationkey = 1");
    QueryContext defaultContext = LocalTajoTestingUtility.createDummyContext(util.getConfiguration());
    LogicalPlan newPlan = planner.createPlan(defaultContext, expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();

    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;

    ProjectionNode projNode = root.getChild();

    assertEquals(NodeType.SELECTION, projNode.getChild().getType());
    SelectionNode selNode = projNode.getChild();
    assertTrue(selNode.hasQual());

    assertEquals(NodeType.SCAN, selNode.getChild().getType());
    ScanNode scanNode = selNode.getChild();
    scanNode.setQual(selNode.getQual());

    PartitionedTableRewriter rewriter = new PartitionedTableRewriter();
    rewriter.setCatalog(catalog);

    PartitionPruningHandle partitionPruningHandle = rewriter.getPartitionPruningHandle(conf, scanNode);
    assertNotNull(partitionPruningHandle);

    Stream<Path> partitionPathStream = Stream.of(partitionPruningHandle.getPartitionPaths())
      .sorted((path1, path2) -> path1.compareTo(path2));
    List<Path> partitionPathList = partitionPathStream.collect(Collectors.toList());
    assertEquals(3, partitionPathList.size());
    assertTrue(partitionPathList.get(0).toString().endsWith("key=part123"));
    assertTrue(partitionPathList.get(1).toString().endsWith("key=part456"));
    assertTrue(partitionPathList.get(2).toString().endsWith("key=part789"));

    Stream<String> partitionKeysStream = Stream.of(partitionPruningHandle.getPartitionKeys())
      .sorted((keys1, keys2) -> keys1.compareTo(keys2));
    List<String> partitionKeysList = partitionKeysStream.collect(Collectors.toList());
    assertEquals(3, partitionKeysList.size());
    assertEquals(partitionKeysList.get(0), "key=part123");
    assertEquals(partitionKeysList.get(1), "key=part456");
    assertEquals(partitionKeysList.get(2), "key=part789");

    assertEquals(33L, partitionPruningHandle.getTotalVolume());
  }

  @Test
  public void testFilterIncludeEveryPartitionKeyColumn() throws Exception {
    Expr expr = analyzer.parse("SELECT * FROM " + MULTIPLE_PARTITION_TABLE_NAME
      + " WHERE key1 = 'part789' and key2 = 'supp789' and key3=3");
    QueryContext defaultContext = LocalTajoTestingUtility.createDummyContext(util.getConfiguration());
    LogicalPlan newPlan = planner.createPlan(defaultContext, expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();

    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;

    ProjectionNode projNode = root.getChild();

    assertEquals(NodeType.SELECTION, projNode.getChild().getType());
    SelectionNode selNode = projNode.getChild();
    assertTrue(selNode.hasQual());

    assertEquals(NodeType.SCAN, selNode.getChild().getType());
    ScanNode scanNode = selNode.getChild();
    scanNode.setQual(selNode.getQual());

    PartitionedTableRewriter rewriter = new PartitionedTableRewriter();
    rewriter.setCatalog(catalog);

    PartitionPruningHandle partitionPruningHandle = rewriter.getPartitionPruningHandle(conf, scanNode);
    assertNotNull(partitionPruningHandle);

    Path[] filteredPaths = partitionPruningHandle.getPartitionPaths();
    assertEquals(1, filteredPaths.length);
    assertEquals("key3=3", filteredPaths[0].getName());
    assertEquals("key2=supp789", filteredPaths[0].getParent().getName());
    assertEquals("key1=part789", filteredPaths[0].getParent().getParent().getName());

    String[] partitionKeys = partitionPruningHandle.getPartitionKeys();
    assertEquals(1, partitionKeys.length);
    assertEquals("key1=part789/key2=supp789/key3=3", partitionKeys[0]);

    assertEquals(10L, partitionPruningHandle.getTotalVolume());
  }

  @Test
  public void testFilterIncludeSomeOfPartitionKeyColumns() throws Exception {
    Expr expr = analyzer.parse("SELECT * FROM " + MULTIPLE_PARTITION_TABLE_NAME
      + " WHERE key1 = 'part123' and key2 = 'supp123' order by n_nationkey");
    QueryContext defaultContext = LocalTajoTestingUtility.createDummyContext(util.getConfiguration());
    LogicalPlan newPlan = planner.createPlan(defaultContext, expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();

    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;

    ProjectionNode projNode = root.getChild();

    assertEquals(NodeType.SORT, projNode.getChild().getType());
    SortNode sortNode = projNode.getChild();

    assertEquals(NodeType.SELECTION, sortNode.getChild().getType());
    SelectionNode selNode = sortNode.getChild();
    assertTrue(selNode.hasQual());

    assertEquals(NodeType.SCAN, selNode.getChild().getType());
    ScanNode scanNode = selNode.getChild();
    scanNode.setQual(selNode.getQual());

    PartitionedTableRewriter rewriter = new PartitionedTableRewriter();
    rewriter.setCatalog(catalog);

    PartitionPruningHandle partitionPruningHandle = rewriter.getPartitionPruningHandle(conf, scanNode);
    assertNotNull(partitionPruningHandle);

    Stream<Path> partitionPathStream = Stream.of(partitionPruningHandle.getPartitionPaths())
      .sorted((path1, path2) -> path1.compareTo(path2));
    List<Path> partitionPathList = partitionPathStream.collect(Collectors.toList());
    assertEquals(2, partitionPathList.size());
    assertTrue(partitionPathList.get(0).toString().endsWith("key1=part123/key2=supp123/key3=1"));
    assertTrue(partitionPathList.get(1).toString().endsWith("key1=part123/key2=supp123/key3=2"));

    Stream<String> partitionKeysStream = Stream.of(partitionPruningHandle.getPartitionKeys())
      .sorted((keys1, keys2) -> keys1.compareTo(keys2));
    List<String> partitionKeysList = partitionKeysStream.collect(Collectors.toList());
    assertEquals(2, partitionKeysList.size());
    assertEquals(partitionKeysList.get(0), ("key1=part123/key2=supp123/key3=1"));
    assertEquals(partitionKeysList.get(1), ("key1=part123/key2=supp123/key3=2"));

    assertEquals(23L, partitionPruningHandle.getTotalVolume());
  }

  @Test
  public void testFilterIncludeNonPartitionKeyColumns() throws Exception {
    Expr expr = analyzer.parse("SELECT * FROM " + MULTIPLE_PARTITION_TABLE_NAME
      + " WHERE key1 = 'part123' and n_nationkey >= 2 order by n_nationkey");
    QueryContext defaultContext = LocalTajoTestingUtility.createDummyContext(util.getConfiguration());
    LogicalPlan newPlan = planner.createPlan(defaultContext, expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();

    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;

    ProjectionNode projNode = root.getChild();

    assertEquals(NodeType.SORT, projNode.getChild().getType());
    SortNode sortNode = projNode.getChild();

    assertEquals(NodeType.SELECTION, sortNode.getChild().getType());
    SelectionNode selNode = sortNode.getChild();
    assertTrue(selNode.hasQual());

    assertEquals(NodeType.SCAN, selNode.getChild().getType());
    ScanNode scanNode = selNode.getChild();
    scanNode.setQual(selNode.getQual());

    PartitionedTableRewriter rewriter = new PartitionedTableRewriter();
    rewriter.setCatalog(catalog);

    PartitionPruningHandle partitionPruningHandle = rewriter.getPartitionPruningHandle(conf, scanNode);
    assertNotNull(partitionPruningHandle);

    Stream<Path> partitionPathStream = Stream.of(partitionPruningHandle.getPartitionPaths())
      .sorted((path1, path2) -> path1.compareTo(path2));
    List<Path> partitionPathList = partitionPathStream.collect(Collectors.toList());
    assertEquals(2, partitionPathList.size());
    assertTrue(partitionPathList.get(0).toString().endsWith("key1=part123/key2=supp123/key3=1"));
    assertTrue(partitionPathList.get(1).toString().endsWith("key1=part123/key2=supp123/key3=2"));

    Stream<String> partitionKeysStream = Stream.of(partitionPruningHandle.getPartitionKeys())
      .sorted((keys1, keys2) -> keys1.compareTo(keys2));
    List<String> partitionKeysList = partitionKeysStream.collect(Collectors.toList());
    assertEquals(2, partitionKeysList.size());
    assertEquals(partitionKeysList.get(0), ("key1=part123/key2=supp123/key3=1"));
    assertEquals(partitionKeysList.get(1), ("key1=part123/key2=supp123/key3=2"));

    assertEquals(23L, partitionPruningHandle.getTotalVolume());
  }

}