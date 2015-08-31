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

package org.apache.tajo.storage.mysql;

import com.facebook.presto.hive.shaded.com.google.common.base.Function;
import com.facebook.presto.hive.shaded.com.google.common.collect.Collections2;
import com.facebook.presto.hive.shaded.com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.tajo.catalog.MetadataProvider;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.storage.Tablespace;
import org.apache.tajo.storage.TablespaceManager;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestMySQLMetadataProvider {
  static final String jdbcUrl = EmbedMySQLServer.getInstance().getJdbcUrl();

  @BeforeClass
  public static void setUp() throws Exception {
  }

  @Test
  public void testGetTablespaceName() throws Exception {
    Tablespace tablespace = TablespaceManager.get(jdbcUrl).get();
    MetadataProvider provider = tablespace.getMetadataProvider();
    assertEquals("mysql_cluster", provider.getTablespaceName());
  }

  @Test
  public void testGetDatabaseName() throws Exception {
    Tablespace tablespace = TablespaceManager.get(jdbcUrl).get();
    MetadataProvider provider = tablespace.getMetadataProvider();
    assertEquals("tpch", provider.getDatabaseName());
  }

  @Test
  public void testGetSchemas() throws Exception {
    Tablespace tablespace = TablespaceManager.get(jdbcUrl).get();
    MetadataProvider provider = tablespace.getMetadataProvider();
    assertTrue(provider.getSchemas().isEmpty());
  }

  //@Test
  public void testGetTables() throws Exception {
    Tablespace tablespace = TablespaceManager.get(jdbcUrl).get();
    MetadataProvider provider = tablespace.getMetadataProvider();

    List<String> tables = Lists.newArrayList(provider.getTables(null, null));
    Collection<String> uppercases = Collections2.transform(tables, new Function<String, String>() {
      @Override
      public String apply(String s) {
        return s.toLowerCase();
      }
    });
    Set<String> found = Sets.newHashSet(uppercases);
    assertEquals(Sets.newHashSet(EmbedMySQLServer.TPCH_TABLES), found);
  }

  //@Test
  public void testGetTableDescriptor() throws Exception {
    Tablespace tablespace = TablespaceManager.get(jdbcUrl).get();
    MetadataProvider provider = tablespace.getMetadataProvider();

    for (String tableName : EmbedMySQLServer.TPCH_TABLES) {
      TableDesc table = provider.getTableDescriptor(null, tableName.toUpperCase());
      assertEquals("tpch." + tableName.toUpperCase(), table.getName());
      assertEquals(jdbcUrl + "&table=" + tableName.toUpperCase(), table.getUri().toASCIIString());
    }
  }
}
