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

package org.apache.tajo.plan;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.plan.logical.GroupbyNode;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.ScanNode;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestLogicalNode {

  @Test
  public void testEquals() {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    schema.addColumn("age", Type.INT2);
    GroupbyNode groupbyNode = new GroupbyNode(0);
    groupbyNode.setGroupingColumns(new Column[]{schema.getColumn(1), schema.getColumn(2)});
    ScanNode scanNode = new ScanNode(0);
    scanNode.init(CatalogUtil.newTableDesc("in", schema, CatalogUtil.newTableMeta("CSV"), new Path("in")));

    GroupbyNode groupbyNode2 = new GroupbyNode(0);
    groupbyNode2.setGroupingColumns(new Column[]{schema.getColumn(1), schema.getColumn(2)});
    JoinNode joinNode = new JoinNode(0);
    ScanNode scanNode2 = new ScanNode(0);
    scanNode2.init(CatalogUtil.newTableDesc("in2", schema, CatalogUtil.newTableMeta("CSV"), new Path("in2")));

    groupbyNode.setChild(scanNode);
    groupbyNode2.setChild(joinNode);
    joinNode.setLeftChild(scanNode);
    joinNode.setRightChild(scanNode2);

    assertTrue(groupbyNode.equals(groupbyNode2));
    assertFalse(groupbyNode.deepEquals(groupbyNode2));

    ScanNode scanNode3 = new ScanNode(0);
    scanNode3.init(CatalogUtil.newTableDesc("in", schema, CatalogUtil.newTableMeta("CSV"), new Path("in")));
    groupbyNode2.setChild(scanNode3);

    assertTrue(groupbyNode.equals(groupbyNode2));
    assertTrue(groupbyNode.deepEquals(groupbyNode2));
  }
}
