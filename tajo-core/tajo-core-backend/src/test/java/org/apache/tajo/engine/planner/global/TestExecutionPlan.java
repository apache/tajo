/*
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

package org.apache.tajo.engine.planner.global;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.engine.planner.LogicalPlan.PIDFactory;
import org.apache.tajo.engine.planner.logical.*;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestExecutionPlan {

  @Test
  public void testJson() {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    schema.addColumn("age", Type.INT2);

    PIDFactory pidFactory = new PIDFactory();
    GroupbyNode groupbyNode = new GroupbyNode(pidFactory.newPID(),
        new Column[]{schema.getColumn(1), schema.getColumn(2)});
    ScanNode scanNode = new ScanNode(pidFactory.newPID(),
        CatalogUtil.newTableDesc("in", schema, CatalogUtil.newTableMeta(StoreType.CSV), new Path("in")));

    groupbyNode.setChild(scanNode);

    ExecutionPlan plan = new ExecutionPlan(pidFactory);
    plan.addPlan(groupbyNode);

    String json = plan.toJson();
    ExecutionPlan fromJson = CoreGsonHelper.fromJson(json, ExecutionPlan.class);
    assertEquals(plan, fromJson);
  }

  @Test
  public void testAddPlan() {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    schema.addColumn("age", Type.INT2);
    PIDFactory pidFactory = new PIDFactory();

    LogicalRootNode root1 = new LogicalRootNode(pidFactory.newPID());
    GroupbyNode groupbyNode = new GroupbyNode(pidFactory.newPID(),
        new Column[]{schema.getColumn(1), schema.getColumn(2)});
    ScanNode scanNode = new ScanNode(pidFactory.newPID(),
        CatalogUtil.newTableDesc("in", schema, CatalogUtil.newTableMeta(StoreType.CSV), new Path("in")));
    root1.setChild(groupbyNode);
    groupbyNode.setChild(scanNode);

    LogicalRootNode root2 = new LogicalRootNode(pidFactory.newPID());
    SortNode sortNode = new SortNode(pidFactory.newPID(),
        new SortSpec[]{new SortSpec(schema.getColumn(2))});
    root2.setChild(sortNode);
    sortNode.setChild(scanNode);

    LogicalRootNode root3 = new LogicalRootNode(pidFactory.newPID());
    JoinNode joinNode = new JoinNode(pidFactory.newPID());
    ScanNode scanNode2 = new ScanNode(pidFactory.newPID(),
        CatalogUtil.newTableDesc("in2", schema, CatalogUtil.newTableMeta(StoreType.CSV), new Path("in2")));
    root3.setChild(joinNode);
    joinNode.setLeftChild(scanNode);
    joinNode.setRightChild(scanNode2);

    ExecutionPlan plan = new ExecutionPlan(pidFactory);
    plan.addPlan(root1);
    plan.addPlan(root2);
    assertEquals(1, plan.getInputContext().size());
    assertEquals(1, plan.getChildCount(groupbyNode));
    assertEquals(1, plan.getChildCount(sortNode));
    assertEquals(plan.getChild(groupbyNode, 0), plan.getChild(sortNode, 0));

//    plan.clear();
    plan.addPlan(root3);
    assertEquals(2, plan.getInputContext().size());
    assertEquals(3, plan.getParentCount(scanNode));

  }

  @Test
  public void testToLinkedLogicalNode() throws CloneNotSupportedException {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    schema.addColumn("age", Type.INT2);
    PIDFactory pidFactory = new PIDFactory();

    LogicalRootNode root1 = new LogicalRootNode(pidFactory.newPID());
    GroupbyNode groupbyNode = new GroupbyNode(pidFactory.newPID(),
        new Column[]{schema.getColumn(1), schema.getColumn(2)});
    ScanNode scanNode = new ScanNode(pidFactory.newPID(),
        CatalogUtil.newTableDesc("in", schema, CatalogUtil.newTableMeta(StoreType.CSV), new Path("in")));
    ScanNode scanNode2 = new ScanNode(pidFactory.newPID(),
        CatalogUtil.newTableDesc("in", schema, CatalogUtil.newTableMeta(StoreType.CSV), new Path("in")));
    UnionNode unionNode = new UnionNode(pidFactory.newPID(), groupbyNode, scanNode2);
    unionNode.setOutSchema(schema);
    unionNode.setInSchema(schema);
    TableSubQueryNode tableSubQueryNode = new TableSubQueryNode(pidFactory.newPID(), "test", unionNode);
    root1.setChild(tableSubQueryNode);
    groupbyNode.setChild(scanNode);

    LogicalRootNode clone = (LogicalRootNode) root1.clone();

    ExecutionPlan plan = new ExecutionPlan(pidFactory);
    plan.addPlan(root1);

    assertTrue(clone.getChild().deepEquals(plan.getFirstPlanGroup().toLinkedLogicalNode()));
  }
}
