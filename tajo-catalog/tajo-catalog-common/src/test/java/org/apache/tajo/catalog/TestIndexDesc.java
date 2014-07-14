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

import org.apache.tajo.catalog.IndexDesc.IndexKey;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexDescProto;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexMethod;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.util.TUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.junit.Assert.*;

public class TestIndexDesc {
  static IndexDesc desc1;
  static IndexDesc desc2;
  static IndexDesc desc3;
  static List<IndexKey> keys1;
  static List<IndexKey> keys2;
  static List<IndexKey> keys3;
  static String pred2;

  static {
    keys1 = TUtil.newList();
    keys1.add(new IndexKey("{\"name\":\"id\",\"dataType\":{\"type\":\"INT4\"}}", true, true));
    desc1 = new IndexDesc("idx_test", DEFAULT_DATABASE_NAME, "indexed",
        IndexMethod.TWO_LEVEL_BIN_TREE, keys1, true, true, null);

    keys2 = TUtil.newList();
    keys2.add(new IndexKey("{\"name\":\"score\",\"dataType\":{\"type\":\"FLOAT8\"}}", false, false));
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
    desc2 = new IndexDesc("idx_test2", DEFAULT_DATABASE_NAME, "indexed",
        IndexMethod.TWO_LEVEL_BIN_TREE, keys2, false, false, pred2);

    keys3 = TUtil.newList();
    keys3.add(new IndexKey("{\"name\":\"id\",\"dataType\":{\"type\":\"INT4\"}}", true, true));
    desc3 = new IndexDesc("idx_test", DEFAULT_DATABASE_NAME, "indexed",
        IndexMethod.TWO_LEVEL_BIN_TREE, keys3, true, true, null);
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
    assertTrue(keys1.equals(desc1.getKeys()));
    assertEquals(true, desc1.isUnique());
    assertEquals(true, desc1.isClustered());
    assertNull(desc1.getPredicate());
    
    assertEquals("idx_test2", desc2.getIndexName());
    assertEquals("indexed", desc2.getTableName());
    assertEquals(IndexMethod.TWO_LEVEL_BIN_TREE, desc2.getIndexMethod());
    assertTrue(keys2.equals(desc2.getKeys()));
    assertEquals(false, desc2.isUnique());
    assertEquals(false, desc2.isClustered());
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
