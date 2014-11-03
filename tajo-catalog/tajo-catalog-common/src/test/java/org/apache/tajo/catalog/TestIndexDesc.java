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

package org.apache.tajo.catalog;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexDescProto;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexMethod;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

public class TestIndexDesc {
  static IndexDesc desc1;
  static IndexDesc desc2;
  static IndexDesc desc3;

  static {
    SortSpec[] colSpecs1 = new SortSpec[1];
    colSpecs1[0] = new SortSpec(new Column("id", Type.INT4), true, true);
    desc1 = new IndexDesc(
        "idx_test", new Path("idx_test"), DEFAULT_DATABASE_NAME, "indexed", colSpecs1,
        IndexMethod.TWO_LEVEL_BIN_TREE, true, true);

    SortSpec[] colSpecs2 = new SortSpec[1];
    colSpecs2[0] = new SortSpec(new Column("score", Type.FLOAT8), false, false);
    desc2 = new IndexDesc(
        "idx_test2", new Path("idx_test2"), DEFAULT_DATABASE_NAME, "indexed", colSpecs2,
        IndexMethod.TWO_LEVEL_BIN_TREE, false, false);

    SortSpec[] colSpecs3 = new SortSpec[1];
    colSpecs3[0] = new SortSpec(new Column("id", Type.INT4), true, false);
    desc3 = new IndexDesc(
        "idx_test", new Path("idx_test"), DEFAULT_DATABASE_NAME, "indexed", colSpecs3,
        IndexMethod.TWO_LEVEL_BIN_TREE, true, true);
  }

  @BeforeClass
  public static void setUp() throws Exception {
  }

  @AfterClass
  public static void tearDown() throws Exception {
  }

  @Test
  public void testIndexDescProto() {
    IndexDescProto proto = desc1.getProto();
    assertEquals(desc1.getProto(), proto);
    assertEquals(desc1, new IndexDesc(proto));
  }

  @Test
  public void testGetFields() {
    assertEquals("idx_test", desc1.getName());
    assertEquals("indexed", desc1.getTableName());
    assertEquals(1, desc1.getColumnSpecs().length);
    assertEquals(new Column("id", Type.INT4), desc1.getColumnSpecs()[0].getSortKey());
    assertEquals(true, desc1.getColumnSpecs()[0].isAscending());
    assertEquals(true, desc1.getColumnSpecs()[0].isNullFirst());
    assertEquals(IndexMethod.TWO_LEVEL_BIN_TREE, desc1.getIndexMethod());
    assertEquals(new Path("idx_test"), desc1.getIndexPath());
    assertEquals(true, desc1.isUnique());
    assertEquals(true, desc1.isClustered());

    assertEquals("idx_test2", desc2.getName());
    assertEquals("indexed", desc2.getTableName());
    assertEquals(1, desc2.getColumnSpecs().length);
    assertEquals(new Column("score", Type.FLOAT8), desc2.getColumnSpecs()[0].getSortKey());
    assertEquals(true, desc2.getColumnSpecs()[0].isAscending());
    assertEquals(true, desc2.getColumnSpecs()[0].isNullFirst());
    assertEquals(IndexMethod.TWO_LEVEL_BIN_TREE, desc2.getIndexMethod());
    assertEquals(new Path("idx_test2"), desc2.getIndexPath());
    assertEquals(false, desc2.isUnique());
    assertEquals(false, desc2.isClustered());
  }

  @Test
  public void testEqualsObject() {
    assertNotSame(desc1, desc2);
    assertEquals(desc1, desc3);
  }

  @Test
  public void testClone() throws CloneNotSupportedException {
    IndexDesc copy = (IndexDesc) desc1.clone();
    assertEquals(desc1, copy);
    assertEquals(desc3, copy);
  }
}
