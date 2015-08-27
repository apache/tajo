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

package org.apache.tajo.catalog;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.exception.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.error.Errors;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.KeyValueSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.junit.Assert.*;

public class TestLinkedMetadataManager {

  static TableDesc TABLE1 = new TableDesc(
      "table1",
      new Schema(new Column[]{new Column("c1", Type.INT8)}),
      "TEXT", new KeyValueSet(), URI.create("http://space1/x/table1")
  );

  static TableDesc TABLE2 = new TableDesc(
      "table2",
          new Schema(new Column[]{new Column("c1", Type.INT8)}),
      "TEXT", new KeyValueSet(), URI.create("http://space1/x/table2")
  );

  static TableDesc TABLE3 = new TableDesc(
      "table3",
      new Schema(new Column[]{new Column("c1", Type.INT8)}),
      "TEXT", new KeyValueSet(), URI.create("http://space1/x/table3")
  );

  static TableDesc TABLE4 = new TableDesc(
      "table4",
          new Schema(new Column[]{new Column("c1", Type.INT8)}),
      "TEXT", new KeyValueSet(), URI.create("http://space1/x/table4")
  );

  static class MockupMetadataProvider1 implements MetadataProvider {

    @Override
    public String getTablespaceName() {
      return "space1";
    }

    @Override
    public URI getTablespaceUri() {
      return URI.create("http://space1/x");
    }

    @Override
    public String getDatabaseName() {
      return "space1";
    }

    @Override
    public Collection<String> getCatalogs() {
      return Lists.newArrayList("cat1", "cat2");
    }

    @Override
    public Collection<String> getTables(@Nullable String catalog) {
      return Lists.newArrayList("table1", "table2");
    }

    @Override
    public TableDesc getTableDescriptor(String catalogName, String tableName) throws UndefinedTablespaceException {
      if (tableName.equals("table1")) {
        return TABLE1;
      } else if (tableName.equals("table2")) {
        return TABLE2;
      }

      throw new UndefinedTablespaceException(tableName);
    }
  }

  static class MockupMetadataProvider2 implements MetadataProvider {

    @Override
    public String getTablespaceName() {
      return "space2";
    }

    @Override
    public URI getTablespaceUri() {
      return URI.create("http://space2/y");
    }

    @Override
    public String getDatabaseName() {
      return "space2";
    }

    @Override
    public Collection<String> getCatalogs() {
      return Lists.newArrayList("cat3", "cat4");
    }

    @Override
    public Collection<String> getTables(@Nullable String catalog) {
      return Lists.newArrayList("table3", "table4");
    }

    @Override
    public TableDesc getTableDescriptor(String catalogName, String tableName) throws UndefinedTablespaceException {
      if (tableName.equals("table3")) {
        return TABLE3;
      } else if (tableName.equals("table4")) {
        return TABLE4;
      }

      throw new UndefinedTablespaceException(tableName);
    }
  }

  static CatalogServer server;
  static CatalogService catalog;

  @BeforeClass
  public static void setUp() throws IOException, DuplicateTablespaceException, DuplicateDatabaseException,
      UnsupportedCatalogStore {
    TajoConf conf = new TajoConf();
    conf.setVar(TajoConf.ConfVars.CATALOG_ADDRESS, "127.0.0.1:0");

    server = new CatalogServer(
        Sets.newHashSet(new MockupMetadataProvider1(), new MockupMetadataProvider2()), Collections.EMPTY_LIST);
    server.init(TestCatalog.newTajoConfForCatalogTest());
    server.start();
    catalog = new LocalCatalogWrapper(server);

    Path defaultTableSpace = CommonTestingUtil.getTestDir();

    if (!catalog.existTablespace(TajoConstants.DEFAULT_TABLESPACE_NAME)) {
      catalog.createTablespace(TajoConstants.DEFAULT_TABLESPACE_NAME, defaultTableSpace.toUri().toString());
    }
    if (!catalog.existDatabase(DEFAULT_DATABASE_NAME)) {
      catalog.createDatabase(DEFAULT_DATABASE_NAME, TajoConstants.DEFAULT_TABLESPACE_NAME);
    }
  }

  @AfterClass
  public static void tearDown() throws IOException {
    server.stop();
  }

  @Test
  public void testGetTablespaceNames() throws Exception {
    assertEquals(Sets.newHashSet("space1", "space2", "default"), Sets.newHashSet(catalog.getAllTablespaceNames()));
  }

  @Test
  public void testGetTablespace() throws Exception {
    CatalogProtos.TablespaceProto space1 = catalog.getTablespace("space1");
    assertEquals("space1", space1.getSpaceName());
    assertEquals("http://space1/x", space1.getUri());

    CatalogProtos.TablespaceProto space2 = catalog.getTablespace("space2");
    assertEquals("space2", space2.getSpaceName());
    assertEquals("http://space2/y", space2.getUri());
  }

  @Test
  public void testGetTablespaces() throws Exception {
    Collection<String> names = Collections2.transform(catalog.getAllTablespaces(),
        new Function<CatalogProtos.TablespaceProto, String>() {
      @Override
      public String apply(@Nullable CatalogProtos.TablespaceProto input) {
        return input.getSpaceName();
      }
    });

    assertEquals(Sets.newHashSet("space1", "space2", "default"), Sets.newHashSet(names));
  }

  @Test
  public void testGetDatabases() throws Exception {
    assertEquals(Sets.newHashSet("space1", "space2", "default", "information_schema"),
        Sets.newHashSet(catalog.getAllDatabaseNames()));
  }

  @Test
  public void testExistsDatabase() throws Exception {
    assertTrue(catalog.existDatabase("space1"));
    assertTrue(catalog.existDatabase("space2"));
    assertTrue(catalog.existDatabase("default"));

    assertFalse(catalog.existDatabase("unknown"));
  }

  @Test
  public void testGetTableNames() throws Exception {
    assertEquals(Sets.newHashSet("table1", "table2"), Sets.newHashSet(catalog.getAllTableNames("space1")));
  }

  @Test(expected = InsufficientPrivilegeException.class)
  public void testCreateTable() throws Exception {
    TableDesc tb = new TableDesc(
        "space1.errortable",
        new Schema(),
        new TableMeta("x", new KeyValueSet()),
        URI.create("file:///"));

    catalog.createTable(tb);
  }

  @Test(expected = InsufficientPrivilegeException.class)
  public void testDropTable() throws Exception {
    catalog.dropTable("space1.table1");
  }

  @Test
  public void testExistsTable() throws Exception {
    assertTrue(catalog.existsTable("space1", "table1"));
    assertTrue(catalog.existsTable("space1", "table2"));
    assertTrue(catalog.existsTable("space2", "table3"));
    assertTrue(catalog.existsTable("space2", "table4"));
  }

  @Test
  public void testGetTable() throws Exception {
    assertEquals(TABLE1, catalog.getTableDesc("space1", "table1"));
    assertEquals(TABLE2, catalog.getTableDesc("space1", "table2"));
    assertEquals(TABLE3, catalog.getTableDesc("space2", "table3"));
    assertEquals(TABLE4, catalog.getTableDesc("space2", "table4"));
  }
}