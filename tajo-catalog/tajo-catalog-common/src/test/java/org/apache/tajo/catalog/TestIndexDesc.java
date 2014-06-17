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

import org.apache.tajo.catalog.proto.CatalogProtos.IndexDescProto;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexMethod;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.util.TUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.junit.Assert.*;

public class TestIndexDesc {
  static IndexDesc desc1;
  static IndexDesc desc2;
  static IndexDesc desc3;
  static String pred2;

  static {
    desc1 = new IndexDesc(
        "idx_test", DEFAULT_DATABASE_NAME, "indexed",
        IndexMethod.TWO_LEVEL_BIN_TREE, true, true,
        new SortSpec[]{new SortSpec(new Column("id", Type.INT4), true, true)}, null);

    pred2 = "{\n" +
        "  \"LeftExpr\": {\n" +
        "    \"ColumnName\": \"score\",\n" +
        "    \"OpType\": \"Column\"\n" +
        "  },\n" +
        "  \"RightExpr\": {\n" +
        "    \"Value\": \"10\",\n" +
        "    \"ValueType\": \"Unsigned_Integer\",\n" +
        "    \"OpType\": \"Literal\"\n" +
        "  },\n" +
        "  \"OpType\": \"Equals\"\n" +
        "}";
    desc2 = new IndexDesc(
        "idx_test2", DEFAULT_DATABASE_NAME, "indexed",
        IndexMethod.TWO_LEVEL_BIN_TREE, false, false,
        new SortSpec[]{new SortSpec(new Column("score", Type.FLOAT8), false, false)}, pred2);

    desc3 = new IndexDesc(
        "idx_test", DEFAULT_DATABASE_NAME, "indexed",
        IndexMethod.TWO_LEVEL_BIN_TREE, true, true,
        new SortSpec[]{new SortSpec(new Column("id", Type.INT4), true, true)}, null);
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
    assertEquals("idx_test", desc1.getIndexName());
    assertEquals("indexed", desc1.getTableName());
    assertEquals(IndexMethod.TWO_LEVEL_BIN_TREE, desc1.getIndexMethod());
    assertEquals(true, desc1.isUnique());
    assertEquals(true, desc1.isClustered());
    assertTrue(TUtil.checkEquals(new SortSpec[]{new SortSpec(new Column("id", Type.INT4), true, true)},
        desc1.getIndexKeys()));
    assertNull(desc1.getPredicate());
    
    assertEquals("idx_test2", desc2.getIndexName());
    assertEquals("indexed", desc2.getTableName());
    assertEquals(IndexMethod.TWO_LEVEL_BIN_TREE, desc2.getIndexMethod());
    assertEquals(false, desc2.isUnique());
    assertEquals(false, desc2.isClustered());
    assertTrue(TUtil.checkEquals(new SortSpec[]{new SortSpec(new Column("score", Type.FLOAT8), false, false)},
        desc2.getIndexKeys()));
    assertEquals(pred2, desc2.getPredicate());
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
