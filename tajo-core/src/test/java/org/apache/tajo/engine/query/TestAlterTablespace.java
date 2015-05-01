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

package org.apache.tajo.engine.query;

import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestAlterTablespace extends QueryTestCaseBase {

  @Test
  public final void testAlterLocation() throws Exception {
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      //////////////////////////////////////////////////////////////////////////////
      // Create two table spaces
      //////////////////////////////////////////////////////////////////////////////

      assertFalse(catalog.existTablespace("space1"));
      assertTrue(catalog.createTablespace("space1", "hdfs://xxx.com/warehouse"));
      assertTrue(catalog.existTablespace("space1"));

      // pre verification
      CatalogProtos.TablespaceProto space1 = catalog.getTablespace("space1");
      assertEquals("space1", space1.getSpaceName());
      assertEquals("hdfs://xxx.com/warehouse", space1.getUri());

      executeString("ALTER TABLESPACE space1 LOCATION 'hdfs://yyy.com/warehouse';");

      // Verify ALTER TABLESPACE space1
      space1 = catalog.getTablespace("space1");
      assertEquals("space1", space1.getSpaceName());
      assertEquals("hdfs://yyy.com/warehouse", space1.getUri());

      assertTrue(catalog.dropTablespace("space1"));
      assertFalse(catalog.existTablespace("space1"));
    }
  }
}
