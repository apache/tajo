/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.client;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.catalog.SchemaBuilder;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.*;
import org.apache.tajo.util.KeyValueSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertFalse;

public class TestCatalogAdminClientExceptions extends QueryTestCaseBase {
  private static TajoTestingCluster cluster;
  private static TajoClient client;

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = TpchTestBase.getInstance().getTestingCluster();
    client = cluster.newTajoClient();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    client.close();
  }

  @Test(expected = DuplicateDatabaseException.class)
  public final void testCreateDatabase() throws TajoException {
    client.createDatabase("default"); // duplicate database
  }

  @Test
  public final void testExistDatabase() {
    assertFalse(client.existDatabase("unknown-database")); // unknown database
  }

  @Test(expected = UndefinedDatabaseException.class)
  public final void testDropDatabase() throws TajoException {
    client.dropDatabase("unknown-database"); // unknown database
  }

  @Test(expected = UnavailableTableLocationException.class)
  public final void testCreateExternalTableUnavailableLocation() throws TajoException {
    client.createExternalTable("table128237", SchemaBuilder.empty(), URI.create("/tajo/test1bcd"),
        new TableMeta("TEXT", new KeyValueSet()));
  }

  @Test(expected = DuplicateTableException.class)
  public final void testCreateExternalTableDuplicated() throws TajoException {
    client.createExternalTable("default.lineitem", SchemaBuilder.empty(), URI.create("/"),
        new TableMeta("TEXT", new KeyValueSet()));
  }

  @Test(expected = InsufficientPrivilegeException.class)
  public final void testCreateExternalTableInsufficientPrivilege() throws TajoException {
    Path p = TajoConf.getWarehouseDir(conf);
    client.createExternalTable("information_schema.table1237891", SchemaBuilder.empty(), p.toUri(),
        new TableMeta("TEXT", new KeyValueSet()));
  }

  @Test(expected = UndefinedTableException.class)
  public final void testDropTableAbsent() throws UndefinedTableException, InsufficientPrivilegeException {
    client.dropTable("unknown-table"); // unknown table
  }

  @Test(expected = InsufficientPrivilegeException.class)
  public final void testDropTableInsufficient() throws UndefinedTableException, InsufficientPrivilegeException {
    client.dropTable("information_schema.tables"); // cannot be dropped
  }

  @Test(expected = UndefinedTableException.class)
  public final void testGetTableDesc() throws UndefinedTableException {
    client.getTableDesc("unknown-table");
  }
}
