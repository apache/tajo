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

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.rewrite.rules.PartitionedTableRewriter;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.FileUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestPartitionedTableRewriter extends QueryTestCaseBase {

  @Test
  public final void testPartitionPruningUsingManuallyAddedPartition() throws Exception {
    String tableName = "testPartitionPruningUsingManualAddedPartition".toLowerCase();
    String canonicalTableName = CatalogUtil.getCanonicalTableName("\"TestPartitionedTableRewriter\"", tableName);

    executeString("create table " + canonicalTableName + " (n_nationkey INT8, n_name TEXT, n_regionkey INT8) " +
      "partition by column(key TEXT)");

    TableDesc tableDesc = catalog.getTableDesc(getCurrentDatabase(), tableName);
    assertNotNull(tableDesc);

    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(tableDesc.getUri().toString() + "/key=part123");
    fs.mkdirs(path);
    FileUtil.writeTextToFile("1|ARGENTINA|1", new Path(path, "data"));

    path = new Path(tableDesc.getUri().toString() + "/key=part456");
    fs.mkdirs(path);
    FileUtil.writeTextToFile("2|BRAZIL|1", new Path(path, "data"));

    path = new Path(tableDesc.getUri().toString() + "/key=part789");
    fs.mkdirs(path);
    FileUtil.writeTextToFile("3|CANADA|1", new Path(path, "data"));

    // Get all partitions
    verifyFindFilteredPartitionPaths("SELECT * FROM " + canonicalTableName + " ORDER BY key", 3);

    // Get partition with filter condition
    verifyFindFilteredPartitionPaths("SELECT * FROM " + canonicalTableName + " WHERE key = 'part456' ORDER BY key", 4);

    executeString("DROP TABLE " + canonicalTableName + " PURGE").close();
  }

  @Test
  public final void testPartitionPruningUsingDirectories() throws Exception {
    String tableName = "testPartitionPruningUsingDirectories".toLowerCase();
    String canonicalTableName = CatalogUtil.getCanonicalTableName("\"TestPartitionedTableRewriter\"", tableName);

    executeString(
      "create table " + canonicalTableName + "(col1 int4, col2 int4) partition by column(key float8) "
        + " as select l_orderkey, l_partkey, l_quantity from default.lineitem");

    TableDesc tableDesc = catalog.getTableDesc(getCurrentDatabase(), tableName);
    assertNotNull(tableDesc);

    // Get all partitions
    verifyFindFilteredPartitionPaths("SELECT * FROM " + canonicalTableName + " ORDER BY key", 1);

    // Get partition with filter condition
    verifyFindFilteredPartitionPaths("SELECT * FROM " + canonicalTableName + " WHERE key = 17.0 ORDER BY key", 2);

    executeString("DROP TABLE " + canonicalTableName + " PURGE").close();
  }

  private void verifyFindFilteredPartitionPaths(String sql, int type) throws Exception {
    Expr expr = sqlParser.parse(sql);
    QueryContext defaultContext = LocalTajoTestingUtility.createDummyContext(testingCluster.getConfiguration());
    LogicalPlan newPlan = planner.createPlan(defaultContext, expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();

    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;

    ProjectionNode projNode = root.getChild();

    assertEquals(NodeType.SORT, projNode.getChild().getType());
    SortNode sortNode = projNode.getChild();

    ScanNode scanNode = null;
    if(type == 2 || type == 4) {
      assertEquals(NodeType.SELECTION, sortNode.getChild().getType());
      SelectionNode selNode = sortNode.getChild();
      assertTrue(selNode.hasQual());

      assertEquals(NodeType.SCAN, selNode.getChild().getType());
      scanNode = selNode.getChild();
      scanNode.setQual(selNode.getQual());
    } else {
      assertEquals(NodeType.SCAN, sortNode.getChild().getType());
      scanNode = sortNode.getChild();
    }

    PartitionedTableRewriter rewriter = new PartitionedTableRewriter();
    OverridableConf conf = CommonTestingUtil.getSessionVarsForTest();

    Path[] filteredPaths = rewriter.findFilteredPartitionPaths(conf, scanNode);
    assertNotNull(filteredPaths);

    if (type == 1) {
      assertEquals(5, filteredPaths.length);
      assertEquals("key=17.0", filteredPaths[0].getName());
      assertEquals("key=36.0", filteredPaths[1].getName());
      assertEquals("key=38.0", filteredPaths[2].getName());
      assertEquals("key=45.0", filteredPaths[3].getName());
      assertEquals("key=49.0", filteredPaths[4].getName());
    } else if (type == 2){
      assertEquals(1, filteredPaths.length);
      assertEquals("key=17.0", filteredPaths[0].getName());
    } else if (type == 3){
      assertEquals(3, filteredPaths.length);
      assertEquals("key=part123", filteredPaths[0].getName());
      assertEquals("key=part456", filteredPaths[1].getName());
      assertEquals("key=part789", filteredPaths[2].getName());
    } else {
      assertEquals(1, filteredPaths.length);
      assertEquals("key=part456", filteredPaths[0].getName());
    }
  }
}
