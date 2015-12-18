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

package org.apache.tajo.catalog.store;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.CatalogConstants;
import org.apache.tajo.catalog.store.object.DatabaseObject;
import org.apache.tajo.catalog.store.object.DatabaseObjectType;
import org.apache.tajo.catalog.store.object.SchemaPatch;
import org.apache.tajo.util.CommonTestingUtil;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.InitializationError;

public class TestXMLCatalogSchemaManager {
  
  private static Path testPath;
  private Path testDatabasePath;
  private Connection conn;
  private Driver driver;
  
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  @interface SetupPrepMethods {
    boolean makeJDBCConnection() default true;
  }
  
  @Rule
  public ExternalResource resource = new ExternalResource() {
    
    private Description description;

    @Override
    public org.junit.runners.model.Statement apply(org.junit.runners.model.Statement base, 
        Description description) {
      this.description = description;
      return super.apply(base, description);
    }

    @Override
    protected void before() throws Throwable {
      SetupPrepMethods prepMethod = this.description.getAnnotation(SetupPrepMethods.class);
      
      if (prepMethod == null || prepMethod.makeJDBCConnection()) {
        setUpJDBC();
      }
    }

    @Override
    protected void after() {
      SetupPrepMethods prepMethod = this.description.getAnnotation(SetupPrepMethods.class);
      
      if (prepMethod == null || prepMethod.makeJDBCConnection()) {
        try {
          tearDownJDBC();
        } catch (Exception e) {
          fail(e.getMessage());
        }
      }
    }
    
  };
  
  private class CollectionMatcher<T> extends TypeSafeDiagnosingMatcher<Iterable<? extends T>> {
    
    private final Matcher<? extends T> matcher;
    
    public CollectionMatcher(Matcher<? extends T> matcher) {
      this.matcher = matcher;
    }

    @Override
    public void describeTo(org.hamcrest.Description description) {
      description.appendText("a collection containing ").appendDescriptionOf(this.matcher);
    }

    @Override
    protected boolean matchesSafely(Iterable<? extends T> item, org.hamcrest.Description mismatchDescription) {
      boolean isFirst = true;

      for (T obj : item) {
        if (this.matcher.matches(obj)) {
          return true;
        }

        if (!isFirst) {
          mismatchDescription.appendText(", ");
        }

        this.matcher.describeMismatch(obj, mismatchDescription);
        isFirst = false;
      }
      return false;
    }
    
  }
  
  private <T> Matcher<Iterable<? extends T>> hasItem(Matcher<? extends T> matcher) {
    return new CollectionMatcher<>(matcher);
  }
  
  @BeforeClass
  public static void setUpClass() throws Exception {
    testPath = CommonTestingUtil.getTestDir();
  }
  
  @AfterClass
  public static void tearDownClass() throws Exception {
    CommonTestingUtil.cleanupTestDir(testPath.toUri().getPath());
  }

  public void setUpJDBC() throws Exception {
    testDatabasePath = CommonTestingUtil.getTestDir();

    driver = new org.apache.derby.jdbc.EmbeddedDriver();
    DriverManager.registerDriver(driver);
    conn = DriverManager.getConnection("jdbc:derby:"+testDatabasePath.toUri().getPath()+"/db;create=true");
    
    if (conn == null) {
      throw new InitializationError("JDBC connection is null.");
    }
  }
  
  public void tearDownJDBC() throws Exception {
    try {
      DriverManager.getConnection("jdbc:derby:;shutdown=true");
    } catch (SQLException se) { 
      if ((se.getErrorCode() != 50000) || (!se.getSQLState().equals("XJ015"))) {
        throw se;
      }
    }
    
    conn = null;
    CommonTestingUtil.cleanupTestDir(testDatabasePath.toUri().getPath());
    DriverManager.deregisterDriver(driver);
  }
  
  protected <T> BaseMatcher<T> hasItemInResultSet(final String expected, final String columnName) {
    return new BaseMatcher<T>() {
      private final List<String> results = new ArrayList<>();

      @Override
      public boolean matches(Object item) {
        boolean result = false;
        
        if (item instanceof ResultSet) {
          ResultSet rs = (ResultSet) item;
          
          try {
            while (rs.next()) {
              results.add(rs.getString(columnName));
              if (expected.equals(rs.getString(columnName))) {
                result = true;
                break;
              }
            }
          } catch (SQLException e) {
          }
        }
        
        return result;
      }

      @Override
      public void describeTo(org.hamcrest.Description description) {
        description.appendText(expected);
      }

      @Override
      public void describeMismatch(Object item, org.hamcrest.Description description) {
        description.appendText("were ").appendText(results.toString());
      }
    };
  }
  
  protected Path createTestJar() throws Exception {
    Path jarPath = new Path(testPath, "testxml.jar");
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    JarOutputStream jarOut = new JarOutputStream(new FileOutputStream(new File(jarPath.toUri())), manifest);
    JarEntry entry = new JarEntry("schemas/jartest/");
    jarOut.putNextEntry(entry);
    jarOut.closeEntry();
    entry = new JarEntry("schemas/jartest/test.xml");
    jarOut.putNextEntry(entry);
    InputStream xmlInputStream = 
        new BufferedInputStream(ClassLoader.getSystemResourceAsStream("schemas/derbytest/loadtest/derby.xml"));
    byte[] buffer = new byte[1024];
    int readSize = -1;
    
    while ((readSize = xmlInputStream.read(buffer)) > -1) {
      jarOut.write(buffer, 0, readSize);
    }
    jarOut.closeEntry();
    jarOut.close();
    xmlInputStream.close();
    
    return jarPath;
  }

  @Test
  @SetupPrepMethods(makeJDBCConnection=false)
  public void testListJarResources() throws Exception {
    XMLCatalogSchemaManager manager;
    Path jarPath;
    URL jarUrl;
    
    jarPath = createTestJar();
    jarUrl = jarPath.toUri().toURL();
    
    URLClassLoader classLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
    Method addURLMethod = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
    addURLMethod.setAccessible(true);
    addURLMethod.invoke(classLoader, jarUrl);
    
    assertThat(classLoader.getURLs(), hasItemInArray(jarUrl));
    
    manager = new XMLCatalogSchemaManager("schemas/jartest");
    
    assertThat(manager.isLoaded(), is(true));
    assertNotNull("Base Schema object cannot be null", manager.getCatalogStore().getSchema());
    assertThat(manager.getCatalogStore().getSchema().getObjects(), not(empty()));
  }
  
  @Test
  public void testCreateBaseSchema() throws Exception {
    XMLCatalogSchemaManager manager;
    Statement stmt;
    
    manager = new XMLCatalogSchemaManager("schemas/derbytest/loadtest");
    assertThat(manager.isLoaded(), is(true));
    
    stmt = conn.createStatement();
    stmt.addBatch("create schema " + manager.getCatalogStore().getSchema().getSchemaName());
    stmt.addBatch("set current schema " + manager.getCatalogStore().getSchema().getSchemaName());
    stmt.executeBatch();
    manager.createBaseSchema(conn);
    
    DatabaseMetaData metadata = conn.getMetaData();
    ResultSet tables = 
      metadata.getTables(null, manager.getCatalogStore().getSchema().getSchemaName().toUpperCase(), 
          CatalogConstants.TB_PARTITION_METHODS, new String[] {"TABLE"});
    
    assertTrue(tables.next());
    assertEquals(CatalogConstants.TB_PARTITION_METHODS, tables.getString("TABLE_NAME"));
    
    ResultSet triggers =
        stmt.executeQuery("select a.TRIGGERNAME from SYS.SYSTRIGGERS a inner join SYS.SYSSCHEMAS b " +
            " on a.SCHEMAID = b.SCHEMAID where b.SCHEMANAME = 'TAJO'");
    
    assertThat(triggers, hasItemInResultSet("TABLESPACES_HIST_TRG", "TRIGGERNAME"));
  }
  
  @Test
  public void testIsInitialized() throws Exception {
    XMLCatalogSchemaManager manager;
    Statement stmt;
    
    manager = new XMLCatalogSchemaManager("schemas/derbytest/querytest");
    assertThat(manager.isLoaded(), is(true));
    
    stmt = conn.createStatement();
    stmt.executeUpdate("CREATE SCHEMA " + manager.getCatalogStore().getSchema().getSchemaName());
    stmt.executeUpdate("SET CURRENT SCHEMA " + manager.getCatalogStore().getSchema().getSchemaName());
    stmt.executeUpdate("CREATE TABLE TESTTABLE1 (COL1 INT, COL2 VARCHAR(10))");
    
    assertThat(manager.isInitialized(conn), is(false));
    
    stmt.addBatch("CREATE TABLE TESTTABLE2 (COL1 INT, COL2 VARCHAR(10))");
    stmt.addBatch("CREATE INDEX TESTINDEX1 ON TESTTABLE1 (COL1)");
    stmt.addBatch("CREATE TRIGGER TESTTRIGGER1 " +
        " AFTER INSERT ON TESTTABLE1 " +
        " REFERENCING NEW AS NEWROW " +
        " FOR EACH ROW " +
        "   INSERT INTO TESTTABLE2 " +
        "   (COL1, COL2) " +
        "   VALUES " +
        "   (NEWROW.COL1, NEWROW.COL2)");
    stmt.addBatch("CREATE SEQUENCE TESTSEQ AS INT");
    stmt.addBatch("CREATE VIEW TESTVIEW (TESTTEXT) AS VALUES 'Text1', 'Text2'");
    stmt.executeBatch();
    
    assertThat(manager.isInitialized(conn), is(true));
  }
  
  @Test
  public void testDropBaseSchema() throws Exception {
    XMLCatalogSchemaManager manager;
    DatabaseMetaData meta;
    Statement stmt;
    
    manager = new XMLCatalogSchemaManager("schemas/derbytest/querytest");
    assertThat(manager.isLoaded(), is(true));
    assertThat(manager.getCatalogStore().getDropStatements(), hasSize(1));
    
    stmt = conn.createStatement();
    stmt.addBatch("CREATE SCHEMA " + manager.getCatalogStore().getSchema().getSchemaName());
    stmt.addBatch("SET CURRENT SCHEMA " + manager.getCatalogStore().getSchema().getSchemaName());
    stmt.addBatch("CREATE TABLE TESTTABLE1 (COL1 INT, COL2 VARCHAR(10))");
    stmt.addBatch("CREATE TABLE TESTTABLE2 (COL1 INT, COL2 VARCHAR(10))");
    stmt.addBatch("CREATE INDEX TESTINDEX1 ON TESTTABLE1 (COL1)");
    stmt.addBatch("CREATE TRIGGER TESTTRIGGER1 " +
        " AFTER INSERT ON TESTTABLE1 " +
        " REFERENCING NEW AS NEWROW " +
        " FOR EACH ROW " +
        "   INSERT INTO TESTTABLE2 " +
        "   (COL1, COL2) " +
        "   VALUES " +
        "   (NEWROW.COL1, NEWROW.COL2)");
    stmt.addBatch("CREATE SEQUENCE TESTSEQ AS INT");
    stmt.addBatch("CREATE VIEW TESTVIEW (TESTTEXT) AS VALUES 'Text1', 'Text2'");
    stmt.executeBatch();
    
    meta = conn.getMetaData();
    assertThat(meta.getTables(null, manager.getCatalogStore().getSchema().getSchemaName().toUpperCase(), 
        null, new String[] {"TABLE"}),
        allOf(hasItemInResultSet("TESTTABLE1", "TABLE_NAME"),
            hasItemInResultSet("TESTTABLE2", "TABLE_NAME")));
    assertThat(meta.getIndexInfo(null, manager.getCatalogStore().getSchema().getSchemaName().toUpperCase(), 
        "TESTTABLE1", false, true), hasItemInResultSet("TESTINDEX1", "INDEX_NAME"));
    
    manager.dropBaseSchema(conn);
    
    assertThat(meta.getTables(null, manager.getCatalogStore().getSchema().getSchemaName().toUpperCase(), 
        null, new String[] {"TABLE"}),
        allOf(not(hasItemInResultSet("TESTTABLE1", "TABLE_NAME")),
            not(hasItemInResultSet("TESTTABLE2", "TABLE_NAME"))));
    ResultSet triggers =
        stmt.executeQuery("select a.TRIGGERNAME from SYS.SYSTRIGGERS a inner join SYS.SYSSCHEMAS b " +
            " on a.SCHEMAID = b.SCHEMAID where b.SCHEMANAME = '" +
            manager.getCatalogStore().getSchema().getSchemaName().toUpperCase() + "'");
    assertThat(triggers, not(hasItemInResultSet("TESTTRIGGER1", "TRIGGERNAME")));
    ResultSet views =
        stmt.executeQuery("select a.TABLENAME from SYS.SYSTABLES a inner join SYS.SYSSCHEMAS b " +
            " on a.SCHEMAID = b.SCHEMAID where a.TABLETYPE = 'V' and b.SCHEMANAME = '" +
            manager.getCatalogStore().getSchema().getSchemaName().toUpperCase() + "'");
    assertThat(views, not(hasItemInResultSet("TESTVIEW", "TABLENAME")));
  }
  
  @Test
  public void testCheckExistance() throws Exception {
    XMLCatalogSchemaManager manager;
    Statement stmt;
    
    manager = new XMLCatalogSchemaManager("schemas/derbytest/querytest");
    assertThat(manager.isLoaded(), is(true));
    
    stmt = conn.createStatement();
    stmt.addBatch("create schema " + manager.getCatalogStore().getSchema().getSchemaName());
    stmt.addBatch("set current schema " + manager.getCatalogStore().getSchema().getSchemaName());
    stmt.executeBatch();
    manager.createBaseSchema(conn);
    
    assertThat(manager.checkExistence(conn, DatabaseObjectType.TABLE, "TESTTABLE1"), is(true));
    assertThat(manager.checkExistence(conn, DatabaseObjectType.TABLE, "testtable2"), is(true));
    assertThat(manager.checkExistence(conn, DatabaseObjectType.TABLE, "TESTTABLE3"), is(false));
    assertThat(manager.checkExistence(conn, DatabaseObjectType.INDEX, "testtable1", "TESTINDEX1"), is(true));
    assertThat(manager.checkExistence(conn, DatabaseObjectType.TRIGGER, "TESTTRIGGER1"), is(true));
    assertThat(manager.checkExistence(conn, DatabaseObjectType.SEQUENCE, "TESTSEQ"), is(true));
    assertThat(manager.checkExistence(conn, DatabaseObjectType.VIEW, "TESTVIEW"), is(true));
  }
  
  @Test
  public void testContinueSchemaCreation() throws Exception {
    XMLCatalogSchemaManager manager;
    Statement stmt;
    
    manager = new XMLCatalogSchemaManager("schemas/derbytest/querytest");
    assertThat(manager.isLoaded(), is(true));
    
    stmt = conn.createStatement();
    stmt.addBatch("create schema " + manager.getCatalogStore().getSchema().getSchemaName());
    stmt.addBatch("set current schema " + manager.getCatalogStore().getSchema().getSchemaName());
    stmt.addBatch("CREATE TABLE TESTTABLE1 (COL1 INT, COL2 VARCHAR(10))");
    stmt.addBatch("CREATE INDEX TESTINDEX1 ON TESTTABLE1 (COL1)");
    stmt.executeBatch();
    manager.createBaseSchema(conn);
    
    assertTrue(manager.checkExistence(conn, DatabaseObjectType.TABLE, "TESTTABLE2"));
  }
  
  @Test
  @SetupPrepMethods(makeJDBCConnection=false)
  public void testMergeSchemaFile() throws Exception {
    XMLCatalogSchemaManager manager;
    
    manager = new XMLCatalogSchemaManager("schemas/derbytest/mergetest");
    assertThat(manager.isLoaded(), is(true));
    
    assertThat(manager.getCatalogStore().getSchema().getObjects(), hasSize(4));
    assertThat(manager.getCatalogStore().getSchema().getObjects(),
        hasItem(Matchers.<DatabaseObject>hasProperty("name", is("TESTINDEX1"))));
    assertThat(manager.getCatalogStore().getSchema().getObjects(), 
        hasItem(Matchers.<DatabaseObject>hasProperty("name", is("TESTTRIGGER1"))));
    assertThat(manager.getCatalogStore().getSchema().getObjects(),
        hasItem(Matchers.<DatabaseObject>hasProperty("order", is(3))));
    assertThat(manager.getCatalogStore().getPatches(), hasSize(1));
    assertThat(manager.getCatalogStore().getPatches(),
        hasItem(Matchers.<SchemaPatch>hasProperty("priorVersion", is(1))));
    assertThat(manager.getCatalogStore().getPatches(),
        hasItem(Matchers.<SchemaPatch>hasProperty("nextVersion", is(2))));
  }
  
  @Test
  public void testUpgradeSchemaObjects() throws Exception {
    XMLCatalogSchemaManager manager;
    Statement stmt;
    
    manager = new XMLCatalogSchemaManager("schemas/derbytest/upgradetest");
    assertThat(manager.isLoaded(), is(true));
    assertThat(manager.getCatalogStore().getPatches(), hasSize(1));
    assertThat(manager.getCatalogStore().getPatches().get(0).getObjects(), hasSize(2));
    
    stmt = conn.createStatement();
    stmt.addBatch("create schema " + manager.getCatalogStore().getSchema().getSchemaName());
    stmt.addBatch("set current schema " + manager.getCatalogStore().getSchema().getSchemaName());
    stmt.addBatch("CREATE TABLE TESTTABLE1 (COL1 INT, COL2 VARCHAR(10))");
    stmt.addBatch("CREATE INDEX TESTINDEX1 ON TESTTABLE1 (COL1)");
    stmt.addBatch("CREATE TABLE TESTTABLE2 (COL1 INT, COL2 VARCHAR(10))");
    stmt.executeBatch();
    manager.upgradeBaseSchema(conn, 1);
    
    DatabaseMetaData metadata = conn.getMetaData();
    ResultSet columns = metadata.getColumns(null, 
        manager.getCatalogStore().getSchema().getSchemaName().toUpperCase(), 
        "TESTTABLE2", "COL2");
    
    assertThat(columns.next(), is(true));
    assertThat(columns.getInt("DATA_TYPE"), is(Types.VARCHAR));
    assertThat(columns.getInt("COLUMN_SIZE"), is(20));
    
    columns = metadata.getColumns(null, 
        manager.getCatalogStore().getSchema().getSchemaName().toUpperCase(), 
        "TESTTABLE2", "COL3");
    
    assertThat(columns.next(), is(true));
    assertThat(columns.getInt("DATA_TYPE"), is(Types.VARCHAR));
    assertThat(columns.getInt("COLUMN_SIZE"), is(25));
  }
  
}
