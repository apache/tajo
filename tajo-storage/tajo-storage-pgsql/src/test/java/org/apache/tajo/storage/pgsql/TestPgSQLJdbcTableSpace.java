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

package org.apache.tajo.storage.pgsql;

import org.apache.tajo.catalog.MetadataProvider;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.exception.NotImplementedException;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Tablespace;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.storage.fragment.Fragment;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestPgSQLJdbcTableSpace {
  static String jdbcUrl = EmbedPgSQLServer.getInstance().getJdbcUrl();

  @Test
  public void testTablespaceHandler() throws Exception {
    assertTrue((TablespaceManager.getByName("pgsql_cluster").get()) instanceof PgSQLTablespace);
    assertEquals("pgsql_cluster", (TablespaceManager.getByName("pgsql_cluster").get().getName()));

    assertTrue((TablespaceManager.get(jdbcUrl).get()) instanceof PgSQLTablespace);
    assertTrue((TablespaceManager.get(jdbcUrl + "&table=tb1").get()) instanceof PgSQLTablespace);

    assertEquals(jdbcUrl, TablespaceManager.get(jdbcUrl).get().getUri().toASCIIString());
    assertTrue(TablespaceManager.get(jdbcUrl).get().getMetadataProvider() instanceof PgSQLMetadataProvider);
  }

  @Test(expected = TajoRuntimeException.class)
  public void testCreateTable() throws IOException, TajoException {
    Tablespace space = TablespaceManager.getByName("pgsql_cluster").get();
    space.createTable(null, false);
  }

  @Test(expected = TajoRuntimeException.class)
  public void testDropTable() throws IOException, TajoException {
    Tablespace space = TablespaceManager.getByName("pgsql_cluster").get();
    space.purgeTable(null);
  }

  @Test
  public void testGetSplits() throws IOException, TajoException {
    Tablespace space = TablespaceManager.getByName("pgsql_cluster").get();
    MetadataProvider provider = space.getMetadataProvider();
    TableDesc table = provider.getTableDescriptor(null, "lineitem");
    List<Fragment> fragments = space.getSplits("lineitem", table, null);
    assertNotNull(fragments);
    assertEquals(1, fragments.size());
  }
}
