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

import org.apache.hadoop.fs.Path;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.rewrite.rules.PartitionedTableRewriter;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestPartitionedTableRewriter extends QueryTestCaseBase {
  @Test
  public final void testPartitionPruningUsingDirectories() throws Exception {
    String tableName = "testPartitionPruningUsingDirectories".toLowerCase();
    String canonicalTableName = CatalogUtil.getCanonicalTableName("\"TestPartitionedTableRewriter\"", tableName);

    executeString(
      "create table " + canonicalTableName + "(col1 int4, col2 int4) partition by column(key float8) "
        + " as select l_orderkey, l_partkey, l_quantity from default.lineitem");

    TableDesc tableDesc = catalog.getTableDesc(getCurrentDatabase(), tableName);
    assertNotNull(tableDesc);

    PartitionedTableRewriter rewriter = new PartitionedTableRewriter();
    OverridableConf conf = CommonTestingUtil.getSessionVarsForTest();

    Schema partitionColumns = tableDesc.getPartitionMethod().getExpressionSchema();

    // Get all partitions
    Path[] filteredPaths = rewriter.findFilteredPaths(conf, partitionColumns, null, new Path(tableDesc.getUri()));
    assertNotNull(filteredPaths);
    assertEquals(5, filteredPaths.length);
    assertEquals("key=17.0", filteredPaths[0].getName());
    assertEquals("key=36.0", filteredPaths[1].getName());
    assertEquals("key=38.0", filteredPaths[2].getName());
    assertEquals("key=45.0", filteredPaths[3].getName());
    assertEquals("key=49.0", filteredPaths[4].getName());

    // Get specified partition
    BinaryEval qual = new BinaryEval(EvalType.EQUAL
      , new FieldEval(new Column("key", TajoDataTypes.Type.FLOAT8))
      , new ConstEval(DatumFactory.createFloat8("17.0"))
    );

    filteredPaths = rewriter.findFilteredPaths(conf, partitionColumns, new EvalNode[] {qual},
      new Path(tableDesc.getUri()));
    assertNotNull(filteredPaths);
    assertEquals(1, filteredPaths.length);
    assertEquals("key=17.0", filteredPaths[0].getName());

    executeString("DROP TABLE " + canonicalTableName + " PURGE").close();
  }
}
