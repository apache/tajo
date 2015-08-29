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

import com.google.common.collect.Sets;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.dictionary.InfoSchemaMetadataDictionary;
import org.apache.tajo.catalog.partition.PartitionDesc;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.FunctionType;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexMethod;
import org.apache.tajo.catalog.proto.CatalogProtos.PartitionKeyProto;
import org.apache.tajo.catalog.store.*;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.UndefinedFunctionException;
import org.apache.tajo.exception.UnsupportedCatalogStore;
import org.apache.tajo.function.Function;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.TUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.catalog.proto.CatalogProtos.AlterTablespaceProto;
import static org.apache.tajo.catalog.proto.CatalogProtos.AlterTablespaceProto.AlterTablespaceType;
import static org.apache.tajo.catalog.proto.CatalogProtos.AlterTablespaceProto.SetLocation;
import static org.junit.Assert.*;

public class TestCatalog {
  static final String FieldName1="f1";
	static final String FieldName2="f2";
	static final String FieldName3="f3";	

	Schema schema1;
	
	static CatalogServer server;
	static CatalogService catalog;

  public static TajoConf newTajoConfForCatalogTest() throws IOException, UnsupportedCatalogStore {
    return CatalogTestingUtil.configureCatalog(new TajoConf(), setupClusterTestBuildDir().getAbsolutePath());
  }

  public static File setupClusterTestBuildDir() throws IOException {
    String randomStr = UUID.randomUUID().toString();
    String dirStr = CommonTestingUtil.getTestDir(randomStr).toString();
    File dir = new File(dirStr).getAbsoluteFile();
    // Have it cleaned up on exit
    dir.deleteOnExit();
    return dir;
  }

	@BeforeClass
	public static void setUp() throws Exception {


    Path defaultTableSpace = CommonTestingUtil.getTestDir();

	  server = new CatalogServer();
    server.init(newTajoConfForCatalogTest());
    server.start();
    catalog = new LocalCatalogWrapper(server);
    if (!catalog.existTablespace(TajoConstants.DEFAULT_TABLESPACE_NAME)) {
      catalog.createTablespace(TajoConstants.DEFAULT_TABLESPACE_NAME, defaultTableSpace.toUri().toString());
    }
    if (!catalog.existDatabase(DEFAULT_DATABASE_NAME)) {
      catalog.createDatabase(DEFAULT_DATABASE_NAME, TajoConstants.DEFAULT_TABLESPACE_NAME);
    }

    for(String table : catalog.getAllTableNames(DEFAULT_DATABASE_NAME)) {
      catalog.dropTable(table);
    }
	}

	@AfterClass
	public static void tearDown() throws IOException {
	  server.stop();
	}

  @Test
  public void testGetTablespace() throws Exception {
    //////////////////////////////////////////////////////////////////////////////
    // Create two table spaces
    //////////////////////////////////////////////////////////////////////////////

    assertFalse(catalog.existTablespace("space1"));
    catalog.createTablespace("space1", "hdfs://xxx.com/warehouse");
    assertTrue(catalog.existTablespace("space1"));

    assertFalse(catalog.existTablespace("space2"));
    catalog.createTablespace("space2", "hdfs://yyy.com/warehouse");
    assertTrue(catalog.existTablespace("space2"));

    //////////////////////////////////////////////////////////////////////////////
    // ALTER TABLESPACE space1
    //////////////////////////////////////////////////////////////////////////////

    // pre verification
    CatalogProtos.TablespaceProto space1 = catalog.getTablespace("space1");
    assertEquals("space1", space1.getSpaceName());
    assertEquals("hdfs://xxx.com/warehouse", space1.getUri());

    // ALTER TABLESPACE space1 LOCATION 'hdfs://zzz.com/warehouse';
    AlterTablespaceProto.AlterTablespaceCommand.Builder commandBuilder =
        AlterTablespaceProto.AlterTablespaceCommand.newBuilder();
    commandBuilder.setType(AlterTablespaceType.LOCATION);
    commandBuilder.setLocation(SetLocation.newBuilder().setUri("hdfs://zzz.com/warehouse"));
    AlterTablespaceProto.Builder alter = AlterTablespaceProto.newBuilder();
    alter.setSpaceName("space1");
    alter.addCommand(commandBuilder.build());
    catalog.alterTablespace(alter.build());

    // Verify ALTER TABLESPACE space1
    space1 = catalog.getTablespace("space1");
    assertEquals("space1", space1.getSpaceName());
    assertEquals("hdfs://zzz.com/warehouse", space1.getUri());

    //////////////////////////////////////////////////////////////////////////////
    // ALTER TABLESPACE space1
    //////////////////////////////////////////////////////////////////////////////

    // pre verification
    CatalogProtos.TablespaceProto space2 = catalog.getTablespace("space2");
    assertEquals("space2", space2.getSpaceName());
    assertEquals("hdfs://yyy.com/warehouse", space2.getUri());

    // ALTER TABLESPACE space1 LOCATION 'hdfs://zzz.com/warehouse';
    commandBuilder = AlterTablespaceProto.AlterTablespaceCommand.newBuilder();
    commandBuilder.setType(AlterTablespaceType.LOCATION);
    commandBuilder.setLocation(SetLocation.newBuilder().setUri("hdfs://www.com/warehouse"));
    alter = AlterTablespaceProto.newBuilder();
    alter.setSpaceName("space2");
    alter.addCommand(commandBuilder.build());
    catalog.alterTablespace(alter.build());

    // post verification
    space1 = catalog.getTablespace("space2");
    assertEquals("space2", space1.getSpaceName());
    assertEquals("hdfs://www.com/warehouse", space1.getUri());


    //////////////////////////////////////////////////////////////////////////////
    // Clean up
    //////////////////////////////////////////////////////////////////////////////
    catalog.dropTablespace("space1");
    assertFalse(catalog.existTablespace("space1"));
    catalog.dropTablespace("space2");
    assertFalse(catalog.existTablespace("space2"));
  }

  @Test
  public void testCreateAndDropDatabases() throws Exception {
    assertFalse(catalog.existDatabase("testCreateAndDropDatabases"));
    catalog.createDatabase("testCreateAndDropDatabases", TajoConstants.DEFAULT_TABLESPACE_NAME);
    assertTrue(catalog.existDatabase("testCreateAndDropDatabases"));
    catalog.dropDatabase("testCreateAndDropDatabases");
  }
  
  @Test
  public void testCreateAndDropDatabaseWithCharacterSensitivity() throws Exception {
    assertFalse(catalog.existDatabase("TestDatabase1"));
    assertFalse(catalog.existDatabase("testDatabase1"));
    catalog.createDatabase("TestDatabase1", TajoConstants.DEFAULT_TABLESPACE_NAME);
    assertTrue(catalog.existDatabase("TestDatabase1"));
    assertFalse(catalog.existDatabase("testDatabase1"));
    catalog.createDatabase("testDatabase1", TajoConstants.DEFAULT_TABLESPACE_NAME);
    assertTrue(catalog.existDatabase("TestDatabase1"));
    assertTrue(catalog.existDatabase("testDatabase1"));
    catalog.dropDatabase("TestDatabase1");
    catalog.dropDatabase("testDatabase1");
  }

  @Test
  public void testCreateAndDropManyDatabases() throws Exception {
    List<String> createdDatabases = new ArrayList<String>();
    InfoSchemaMetadataDictionary dictionary = new InfoSchemaMetadataDictionary();
    String namePrefix = "database_";
    final int NUM = 10;
    for (int i = 0; i < NUM; i++) {
      String databaseName = namePrefix + i;
      assertFalse(catalog.existDatabase(databaseName));
      catalog.createDatabase(databaseName, TajoConstants.DEFAULT_TABLESPACE_NAME);
      assertTrue(catalog.existDatabase(databaseName));
      createdDatabases.add(databaseName);
    }

    Collection<String> allDatabaseNames = catalog.getAllDatabaseNames();
    for (String databaseName : allDatabaseNames) {
      assertTrue(databaseName.equals(DEFAULT_DATABASE_NAME) || createdDatabases.contains(databaseName) ||
          dictionary.isSystemDatabase(databaseName));
    }
    // additional ones are 'default' and 'system' databases.
    assertEquals(NUM + 2, allDatabaseNames.size());

    Collections.shuffle(createdDatabases);
    for (String tobeDropped : createdDatabases) {
      assertTrue(catalog.existDatabase(tobeDropped));
      catalog.dropDatabase(tobeDropped);
      assertFalse(catalog.existDatabase(tobeDropped));
    }
  }

  private TableDesc createMockupTable(String databaseName, String tableName) throws IOException {
    schema1 = new Schema();
    schema1.addColumn(FieldName1, Type.BLOB);
    schema1.addColumn(FieldName2, Type.INT4);
    schema1.addColumn(FieldName3, Type.INT8);
    Path path = new Path(CommonTestingUtil.getTestDir(), tableName);
    TableDesc table = new TableDesc(
        CatalogUtil.buildFQName(databaseName, tableName),
        schema1,
        new TableMeta("TEXT", new KeyValueSet()),
        path.toUri(), true);
    return table;
  }

  @Test
  public void testCreateAndDropTable() throws Exception {
    catalog.createDatabase("tmpdb1", TajoConstants.DEFAULT_TABLESPACE_NAME);
    assertTrue(catalog.existDatabase("tmpdb1"));
    catalog.createDatabase("tmpdb2", TajoConstants.DEFAULT_TABLESPACE_NAME);
    assertTrue(catalog.existDatabase("tmpdb2"));

    TableDesc table1 = createMockupTable("tmpdb1", "table1");
    catalog.createTable(table1);

    TableDesc table2 = createMockupTable("tmpdb2", "table2");
    catalog.createTable(table2);

    Set<String> tmpdb1 = Sets.newHashSet(catalog.getAllTableNames("tmpdb1"));
    assertEquals(1, tmpdb1.size());
    assertTrue(tmpdb1.contains("table1"));


    Set<String> tmpdb2 = Sets.newHashSet(catalog.getAllTableNames("tmpdb2"));
    assertEquals(1, tmpdb2.size());
    assertTrue(tmpdb2.contains("table2"));

    catalog.dropDatabase("tmpdb1");
    assertFalse(catalog.existDatabase("tmpdb1"));

    tmpdb2 = Sets.newHashSet(catalog.getAllTableNames("tmpdb2"));
    assertEquals(1, tmpdb2.size());
    assertTrue(tmpdb2.contains("table2"));

    catalog.dropDatabase("tmpdb2");
    assertFalse(catalog.existDatabase("tmpdb2"));
  }
  
  @Test
  public void testCreateAndDropTableWithCharacterSensivity() throws Exception {
    String databaseName = "TestDatabase1";
    catalog.createDatabase(databaseName, TajoConstants.DEFAULT_TABLESPACE_NAME);
    assertTrue(catalog.existDatabase(databaseName));
    
    String tableName = "TestTable1";
    Schema schema = new Schema();
    schema.addColumn("Column", Type.BLOB);
    schema.addColumn("column", Type.INT4);
    schema.addColumn("cOlumn", Type.INT8);
    Path path = new Path(CommonTestingUtil.getTestDir(), tableName);
    TableDesc table = new TableDesc(
        CatalogUtil.buildFQName(databaseName, tableName),
        schema,
        new TableMeta("TEXT", new KeyValueSet()),
        path.toUri(), true);
    
    catalog.createTable(table);
    
    tableName = "testTable1";
    schema = new Schema();
    schema.addColumn("Column", Type.BLOB);
    schema.addColumn("column", Type.INT4);
    schema.addColumn("cOlumn", Type.INT8);
    path = new Path(CommonTestingUtil.getTestDir(), tableName);
    table = new TableDesc(
        CatalogUtil.buildFQName(databaseName, tableName),
        schema,
        new TableMeta("TEXT", new KeyValueSet()),
        path.toUri(), true);
    
    catalog.createTable(table);
    
    catalog.dropDatabase(databaseName);
  }

  static String dbPrefix = "db_";
  static String tablePrefix = "tb_";
  static final int DB_NUM = 5;
  static final int TABLE_NUM_PER_DB = 3;
  static final int TOTAL_TABLE_NUM = DB_NUM * TABLE_NUM_PER_DB;

  private Map<String, List<String>> createBaseDatabaseAndTables() throws IOException, TajoException {

    Map<String, List<String>> createdDatabaseAndTablesMap = new HashMap<String, List<String>>();

    // add and divide all tables to multiple databases in a round robin manner
    for (int tableId = 0; tableId < TOTAL_TABLE_NUM; tableId++) {
      int dbIdx = tableId % DB_NUM;
      String databaseName = dbPrefix + dbIdx;

      if (!catalog.existDatabase(databaseName)) {
        catalog.createDatabase(databaseName, TajoConstants.DEFAULT_TABLESPACE_NAME);
      }

      String tableName = tablePrefix + tableId;
      TableDesc table = createMockupTable(databaseName, tableName);
      catalog.createTable(table);

      TUtil.putToNestedList(createdDatabaseAndTablesMap, databaseName, tableName);
    }

    // checking all tables for each database
    for (int dbIdx = 0; dbIdx < DB_NUM; dbIdx++) {
      String databaseName = dbPrefix + dbIdx;

      Collection<String> tableNames = catalog.getAllTableNames(databaseName);
      assertTrue(createdDatabaseAndTablesMap.containsKey(databaseName));

      assertEquals(createdDatabaseAndTablesMap.get(databaseName).size(), tableNames.size());
      for (String tableName : tableNames) {
        assertTrue(createdDatabaseAndTablesMap.get(databaseName).contains(tableName));
      }
    }

    return createdDatabaseAndTablesMap;
  }

  @Test
  public void testDropDatabaseWithAllTables() throws Exception {
    Map<String, List<String>> createdTablesMap = createBaseDatabaseAndTables();

    // Each time we drop one database, check all databases and their tables.
    Iterator<String> it = new ArrayList<String>(createdTablesMap.keySet()).iterator();
    while(it.hasNext()) {
      // drop one database
      String databaseName = it.next();
      assertTrue(catalog.existDatabase(databaseName));
      catalog.dropDatabase(databaseName);
      createdTablesMap.remove(databaseName);

      // check all tables which belong to other databases
      for (Map.Entry<String, List<String>> entry : createdTablesMap.entrySet()) {
        assertTrue(catalog.existDatabase(entry.getKey()));

        // checking all tables for this database
        Collection<String> tablesForThisDatabase = catalog.getAllTableNames(entry.getKey());
        assertEquals(createdTablesMap.get(entry.getKey()).size(), tablesForThisDatabase.size());
        for (String tableName : tablesForThisDatabase) {
          assertTrue(createdTablesMap.get(entry.getKey()).contains(CatalogUtil.extractSimpleName(tableName)));
        }
      }
    }

    // Finally, default and system database will remain. So, its result is 1.
    assertEquals(2, catalog.getAllDatabaseNames().size());
  }
	
	@Test
	public void testGetTable() throws Exception {
		schema1 = new Schema();
		schema1.addColumn(FieldName1, Type.BLOB);
		schema1.addColumn(FieldName2, Type.INT4);
		schema1.addColumn(FieldName3, Type.INT8);
    Path path = new Path(CommonTestingUtil.getTestDir(), "table1");
    TableDesc meta = new TableDesc(
        CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "getTable"),
        schema1,
        "TEXT",
        new KeyValueSet(),
        path.toUri());

		assertFalse(catalog.existsTable(DEFAULT_DATABASE_NAME, "getTable"));
    catalog.createTable(meta);
    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, "getTable"));

    catalog.dropTable(CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "getTable"));
    assertFalse(catalog.existsTable(DEFAULT_DATABASE_NAME, "getTable"));
	}

  /**
   * It asserts the equality between an original table desc and a restored table desc.
   */
  private static void assertSchemaEquality(String tableName, Schema schema) throws IOException, TajoException {
    Path path = new Path(CommonTestingUtil.getTestDir(), tableName);
    TableDesc tableDesc = new TableDesc(
        CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, tableName),
        schema,
        "TEXT",
        new KeyValueSet(),
        path.toUri());

    // schema creation
    assertFalse(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));
    catalog.createTable(tableDesc);
    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    // change it for the equals test.
    schema.setQualifier(CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, tableName));
    TableDesc restored = catalog.getTableDesc(DEFAULT_DATABASE_NAME, tableName);
    assertEquals(schema, restored.getSchema());

    // drop test
    catalog.dropTable(CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, tableName));
    assertFalse(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));
  }

  @Test
  public void testCreateAndGetNestedTable1() throws Exception {
    // schema creation
    // three level nested schema
    //
    // s1
    //  |- s2
    //  |- s3
    //      |- s4
    //      |- s7
    //          |- s5
    //              |- s6
    //      |- s8
    //  |- s9

    Schema nestedSchema = new Schema();
    nestedSchema.addColumn("s1", Type.INT8);

    nestedSchema.addColumn("s2", Type.INT8);

    Schema s5 = new Schema();
    s5.addColumn("s6", Type.INT8);

    Schema s7 = new Schema();
    s7.addColumn("s5", new TypeDesc(s5));

    Schema s3 = new Schema();
    s3.addColumn("s4", Type.INT8);
    s3.addColumn("s7", new TypeDesc(s7));
    s3.addColumn("s8", Type.INT8);

    nestedSchema.addColumn("s3", new TypeDesc(s3));
    nestedSchema.addColumn("s9", Type.INT8);

    assertSchemaEquality("nested_schema1", nestedSchema);
  }

  @Test
  public void testCreateAndGetNestedTable2() throws Exception {
    // schema creation
    // three level nested schema
    //
    // s1
    //  |- s2
    //  |- s3
    //      |- s1
    //      |- s2
    //          |- s3
    //              |- s1
    //      |- s3
    //  |- s4

    Schema nestedSchema = new Schema();
    nestedSchema.addColumn("s1", Type.INT8);

    nestedSchema.addColumn("s2", Type.INT8);

    Schema s5 = new Schema();
    s5.addColumn("s6", Type.INT8);

    Schema s7 = new Schema();
    s7.addColumn("s5", new TypeDesc(s5));

    Schema s3 = new Schema();
    s3.addColumn("s4", Type.INT8);
    s3.addColumn("s7", new TypeDesc(s7));
    s3.addColumn("s8", Type.INT8);

    nestedSchema.addColumn("s3", new TypeDesc(s3));
    nestedSchema.addColumn("s9", Type.INT8);

    assertSchemaEquality("nested_schema2", nestedSchema);
  }

  static IndexDesc desc1;
  static IndexDesc desc2;
  static IndexDesc desc3;
  static Schema relationSchema;

  public static TableDesc prepareTable() throws IOException {
    relationSchema = new Schema();
    relationSchema.addColumn(DEFAULT_DATABASE_NAME + ".indexed.id", Type.INT4)
        .addColumn(DEFAULT_DATABASE_NAME + ".indexed.name", Type.TEXT)
        .addColumn(DEFAULT_DATABASE_NAME + ".indexed.age", Type.INT4)
        .addColumn(DEFAULT_DATABASE_NAME + ".indexed.score", Type.FLOAT8);

    String tableName = "indexed";

    TableMeta meta = CatalogUtil.newTableMeta("TEXT");
    return new TableDesc(
        CatalogUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, tableName), relationSchema, meta,
        new Path(CommonTestingUtil.getTestDir(), "indexed").toUri());
  }

  public static void prepareIndexDescs() throws IOException, URISyntaxException {
    SortSpec[] colSpecs1 = new SortSpec[1];
    colSpecs1[0] = new SortSpec(new Column("default.indexed.id", Type.INT4), true, true);
    desc1 = new IndexDesc(DEFAULT_DATABASE_NAME, "indexed",
        "idx_test", new URI("idx_test"), colSpecs1,
        IndexMethod.TWO_LEVEL_BIN_TREE, true, true, relationSchema);

    SortSpec[] colSpecs2 = new SortSpec[1];
    colSpecs2[0] = new SortSpec(new Column("default.indexed.score", Type.FLOAT8), false, false);
    desc2 = new IndexDesc(DEFAULT_DATABASE_NAME, "indexed",
        "idx_test2", new URI("idx_test2"), colSpecs2,
        IndexMethod.TWO_LEVEL_BIN_TREE, false, false, relationSchema);

    SortSpec[] colSpecs3 = new SortSpec[1];
    colSpecs3[0] = new SortSpec(new Column("default.indexed.id", Type.INT4), true, false);
    desc3 = new IndexDesc(DEFAULT_DATABASE_NAME, "indexed",
        "idx_test", new URI("idx_test"), colSpecs3,
        IndexMethod.TWO_LEVEL_BIN_TREE, true, true, relationSchema);
  }

  @Test
  public void testCreateSameTables() throws IOException, TajoException {
    catalog.createDatabase("tmpdb3", TajoConstants.DEFAULT_TABLESPACE_NAME);
    assertTrue(catalog.existDatabase("tmpdb3"));
    catalog.createDatabase("tmpdb4", TajoConstants.DEFAULT_TABLESPACE_NAME);
    assertTrue(catalog.existDatabase("tmpdb4"));

    TableDesc table1 = createMockupTable("tmpdb3", "table1");
    catalog.createTable(table1);
    TableDesc table2 = createMockupTable("tmpdb3", "table2");
    catalog.createTable(table2);
    assertTrue(catalog.existsTable("tmpdb3", "table1"));
    assertTrue(catalog.existsTable("tmpdb3", "table2"));

    TableDesc table3 = createMockupTable("tmpdb4", "table1");
    catalog.createTable(table3);
    TableDesc table4 = createMockupTable("tmpdb4", "table2");
    catalog.createTable(table4);
    assertTrue(catalog.existsTable("tmpdb4", "table1"));
    assertTrue(catalog.existsTable("tmpdb4", "table2"));

    catalog.dropTable("tmpdb3.table1");
    catalog.dropTable("tmpdb3.table2");
    catalog.dropTable("tmpdb4.table1");
    catalog.dropTable("tmpdb4.table2");

    assertFalse(catalog.existsTable("tmpdb3.table1"));
    assertFalse(catalog.existsTable("tmpdb3.table2"));
    assertFalse(catalog.existsTable("tmpdb4.table1"));
    assertFalse(catalog.existsTable("tmpdb4.table2"));
  }
	
	@Test
	public void testAddAndDelIndex() throws Exception {
	  TableDesc desc = prepareTable();
    prepareIndexDescs();
	  catalog.createTable(desc);
	  
	  assertFalse(catalog.existIndexByName(DEFAULT_DATABASE_NAME, desc1.getName()));
	  assertFalse(catalog.existIndexByColumnNames(DEFAULT_DATABASE_NAME, "indexed", new String[]{"id"}));
	  catalog.createIndex(desc1);
	  assertTrue(catalog.existIndexByName(DEFAULT_DATABASE_NAME, desc1.getName()));
	  assertTrue(catalog.existIndexByColumnNames(DEFAULT_DATABASE_NAME, "indexed", new String[]{"id"}));


	  assertFalse(catalog.existIndexByName(DEFAULT_DATABASE_NAME, desc2.getName()));
	  assertFalse(catalog.existIndexByColumnNames(DEFAULT_DATABASE_NAME, "indexed", new String[]{"score"}));
	  catalog.createIndex(desc2);
	  assertTrue(catalog.existIndexByName(DEFAULT_DATABASE_NAME, desc2.getName()));
	  assertTrue(catalog.existIndexByColumnNames(DEFAULT_DATABASE_NAME, "indexed", new String[]{"score"}));

    Set<IndexDesc> indexDescs = TUtil.newHashSet();
    indexDescs.add(desc1);
    indexDescs.add(desc2);
    indexDescs.add(desc3);
    for (IndexDesc index : catalog.getAllIndexesByTable(DEFAULT_DATABASE_NAME, "indexed")) {
      assertTrue(indexDescs.contains(index));
    }
	  
	  catalog.dropIndex(DEFAULT_DATABASE_NAME, desc1.getName());
	  assertFalse(catalog.existIndexByName(DEFAULT_DATABASE_NAME, desc1.getName()));
	  catalog.dropIndex(DEFAULT_DATABASE_NAME, desc2.getName());
	  assertFalse(catalog.existIndexByName(DEFAULT_DATABASE_NAME, desc2.getName()));
	  
	  catalog.dropTable(desc.getName());
    assertFalse(catalog.existsTable(desc.getName()));
  }
	
	public static class TestFunc1 extends Function {
		public TestFunc1() {
			super(					
					new Column [] {
							new Column("name", TajoDataTypes.Type.INT4)
					}
			);
		}

    public CatalogProtos.FunctionType getFunctionType() {
      return FunctionType.GENERAL;
    }
	}

  public static class TestFunc2 extends Function {
    public TestFunc2() {
      super(
          new Column [] {
              new Column("name", TajoDataTypes.Type.INT4),
              new Column("bytes", TajoDataTypes.Type.BLOB)
          }
      );
    }
    public CatalogProtos.FunctionType getFunctionType() {
      return FunctionType.GENERAL;
    }
  }

  @Test
  public final void testRegisterAndFindFunc() throws Exception {
    assertFalse(catalog.containFunction("test10", FunctionType.GENERAL));
    FunctionDesc meta = new FunctionDesc("test10", TestFunc2.class, FunctionType.GENERAL,
        CatalogUtil.newSimpleDataType(Type.INT4),
        CatalogUtil.newSimpleDataTypeArray(Type.INT4, Type.BLOB));

    catalog.createFunction(meta);
    assertTrue(catalog.containFunction("test10", CatalogUtil.newSimpleDataTypeArray(Type.INT4, Type.BLOB)));
    FunctionDesc retrived = catalog.getFunction("test10", CatalogUtil.newSimpleDataTypeArray(Type.INT4, Type.BLOB));

    assertEquals(retrived.getFunctionName(), "test10");
    assertEquals(retrived.getLegacyFuncClass(), TestFunc2.class);
    assertEquals(retrived.getFuncType(), FunctionType.GENERAL);

    assertFalse(catalog.containFunction("test10", CatalogUtil.newSimpleDataTypeArray(Type.BLOB, Type.INT4)));
  }
  

	@Test
	public final void testRegisterFunc() throws Exception { 
		assertFalse(catalog.containFunction("test2", FunctionType.UDF));
		FunctionDesc meta = new FunctionDesc("test2", TestFunc1.class, FunctionType.UDF,
        CatalogUtil.newSimpleDataType(Type.INT4),
        CatalogUtil.newSimpleDataTypeArray(Type.INT4));

    catalog.createFunction(meta);
		assertTrue(catalog.containFunction("test2", CatalogUtil.newSimpleDataTypeArray(Type.INT4)));
		FunctionDesc retrived = catalog.getFunction("test2", CatalogUtil.newSimpleDataTypeArray(Type.INT4));

		assertEquals(retrived.getFunctionName(),"test2");
		assertEquals(retrived.getLegacyFuncClass(),TestFunc1.class);
		assertEquals(retrived.getFuncType(),FunctionType.UDF);
	}

  @Test
  public final void testSuchFunctionException() throws Exception {
    try {
      assertFalse(catalog.containFunction("test123", CatalogUtil.newSimpleDataTypeArray(Type.INT4)));
      catalog.getFunction("test123", CatalogUtil.newSimpleDataTypeArray(Type.INT4));
      fail();
    } catch (UndefinedFunctionException nsfe) {
      // succeed test
    } catch (Throwable e) {
      fail(e.getMessage());
    }
  }

  @Test
  public final void testDropFunction() throws Exception {
    assertFalse(catalog.containFunction("test3", CatalogUtil.newSimpleDataTypeArray(Type.INT4)));
    FunctionDesc meta = new FunctionDesc("test3", TestFunc1.class, FunctionType.UDF,
        CatalogUtil.newSimpleDataType(Type.INT4),
        CatalogUtil.newSimpleDataTypeArray(Type.INT4));
    catalog.createFunction(meta);
    assertTrue(catalog.containFunction("test3", CatalogUtil.newSimpleDataTypeArray(Type.INT4)));
    catalog.dropFunction("test3");
    assertFalse(catalog.containFunction("test3", CatalogUtil.newSimpleDataTypeArray(Type.INT4)));

    assertFalse(catalog.containFunction("test3", CatalogUtil.newSimpleDataTypeArray(Type.INT4, Type.BLOB)));
    FunctionDesc overload = new FunctionDesc("test3", TestFunc2.class, FunctionType.GENERAL,
        CatalogUtil.newSimpleDataType(Type.INT4),
        CatalogUtil.newSimpleDataTypeArray(Type.INT4, Type.BLOB));
    catalog.createFunction(overload);
    assertTrue(catalog.containFunction("test3", CatalogUtil.newSimpleDataTypeArray(Type.INT4, Type.BLOB)));
  }

  @Test
  public final void testAddAndDeleteTablePartitionByHash1() throws Exception {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4)
        .addColumn("name", Type.TEXT)
        .addColumn("age", Type.INT4)
        .addColumn("score", Type.FLOAT8);

    String tableName = CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "addedtable");
    KeyValueSet opts = new KeyValueSet();
    opts.set("file.delimiter", ",");
    TableMeta meta = CatalogUtil.newTableMeta("TEXT", opts);


    Schema partSchema = new Schema();
    partSchema.addColumn("id", Type.INT4);

    PartitionMethodDesc partitionDesc =
        new PartitionMethodDesc(DEFAULT_DATABASE_NAME, tableName,
            CatalogProtos.PartitionType.HASH, "id", partSchema);

    TableDesc desc =
        new TableDesc(tableName, schema, meta,
            new Path(CommonTestingUtil.getTestDir(), "addedtable").toUri());
    desc.setPartitionMethod(partitionDesc);

    assertFalse(catalog.existsTable(tableName));
    catalog.createTable(desc);
    assertTrue(catalog.existsTable(tableName));
    TableDesc retrieved = catalog.getTableDesc(tableName);

    assertEquals(retrieved.getName(), tableName);
    assertEquals(retrieved.getPartitionMethod().getPartitionType(), CatalogProtos.PartitionType.HASH);
    assertEquals(retrieved.getPartitionMethod().getExpressionSchema().getColumn(0).getSimpleName(), "id");

    catalog.dropTable(tableName);
    assertFalse(catalog.existsTable(tableName));
  }


  @Test
  public final void testAddAndDeleteTablePartitionByHash2() throws Exception {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4)
        .addColumn("name", Type.TEXT)
        .addColumn("age", Type.INT4)
        .addColumn("score", Type.FLOAT8);

    String tableName = CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "addedtable");
    KeyValueSet opts = new KeyValueSet();
    opts.set("file.delimiter", ",");
    TableMeta meta = CatalogUtil.newTableMeta("TEXT", opts);

    Schema partSchema = new Schema();
    partSchema.addColumn("id", Type.INT4);
    PartitionMethodDesc partitionDesc =
        new PartitionMethodDesc(DEFAULT_DATABASE_NAME, tableName,
            CatalogProtos.PartitionType.HASH, "id", partSchema);

    TableDesc desc =
        new TableDesc(tableName, schema, meta,
            new Path(CommonTestingUtil.getTestDir(), "addedtable").toUri());
    desc.setPartitionMethod(partitionDesc);

    assertFalse(catalog.existsTable(tableName));
    catalog.createTable(desc);
    assertTrue(catalog.existsTable(tableName));

    TableDesc retrieved = catalog.getTableDesc(tableName);

    assertEquals(retrieved.getName(), tableName);
    assertEquals(retrieved.getPartitionMethod().getPartitionType(), CatalogProtos.PartitionType.HASH);
    assertEquals(retrieved.getPartitionMethod().getExpressionSchema().getColumn(0).getSimpleName(), "id");

    catalog.dropTable(tableName);
    assertFalse(catalog.existsTable(tableName));
  }

  @Test
  public final void testAddAndDeleteTablePartitionByList() throws Exception {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4)
        .addColumn("name", Type.TEXT)
        .addColumn("age", Type.INT4)
        .addColumn("score", Type.FLOAT8);

    String tableName = CatalogUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, "addedtable");
    KeyValueSet opts = new KeyValueSet();
    opts.set("file.delimiter", ",");
    TableMeta meta = CatalogUtil.newTableMeta("TEXT", opts);

    Schema partSchema = new Schema();
    partSchema.addColumn("id", Type.INT4);
    PartitionMethodDesc partitionDesc =
        new PartitionMethodDesc(DEFAULT_DATABASE_NAME, tableName,
            CatalogProtos.PartitionType.LIST, "id", partSchema);

    TableDesc desc =
        new TableDesc(tableName, schema, meta,
            new Path(CommonTestingUtil.getTestDir(), "addedtable").toUri());
    desc.setPartitionMethod(partitionDesc);
    assertFalse(catalog.existsTable(tableName));
    catalog.createTable(desc);
    assertTrue(catalog.existsTable(tableName));

    TableDesc retrieved = catalog.getTableDesc(tableName);

    assertEquals(retrieved.getName(), tableName);
    assertEquals(retrieved.getPartitionMethod().getPartitionType(), CatalogProtos.PartitionType.LIST);
    assertEquals(retrieved.getPartitionMethod().getExpressionSchema().getColumn(0).getSimpleName(), "id");

    catalog.dropTable(tableName);
    assertFalse(catalog.existsTable(tableName));
  }

  @Test
  public final void testAddAndDeleteTablePartitionByRange() throws Exception {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4)
        .addColumn("name", Type.TEXT)
        .addColumn("age", Type.INT4)
        .addColumn("score", Type.FLOAT8);

    String tableName = CatalogUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, "addedtable");
    KeyValueSet opts = new KeyValueSet();
    opts.set("file.delimiter", ",");
    TableMeta meta = CatalogUtil.newTableMeta("TEXT", opts);

    Schema partSchema = new Schema();
    partSchema.addColumn("id", Type.INT4);
    PartitionMethodDesc partitionDesc =
        new PartitionMethodDesc(DEFAULT_DATABASE_NAME, tableName, CatalogProtos.PartitionType.RANGE,
            "id", partSchema);

    TableDesc desc =
        new TableDesc(tableName, schema, meta,
            new Path(CommonTestingUtil.getTestDir(), "addedtable").toUri());
    desc.setPartitionMethod(partitionDesc);
    assertFalse(catalog.existsTable(tableName));
    catalog.createTable(desc);
    assertTrue(catalog.existsTable(tableName));

    TableDesc retrieved = catalog.getTableDesc(tableName);

    assertEquals(retrieved.getName(), tableName);
    assertEquals(retrieved.getPartitionMethod().getPartitionType(), CatalogProtos.PartitionType.RANGE);
    assertEquals(retrieved.getPartitionMethod().getExpressionSchema().getColumn(0).getSimpleName(), "id");

    catalog.dropTable(tableName);
    assertFalse(catalog.existsTable(tableName));
  }

  @Test
  public final void testAddAndDeleteTablePartitionByColumn() throws Exception {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4)
        .addColumn("name", Type.TEXT)
        .addColumn("age", Type.INT4)
        .addColumn("score", Type.FLOAT8);

    String tableName = CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "addedtable");
    KeyValueSet opts = new KeyValueSet();
    opts.set("file.delimiter", ",");
    TableMeta meta = CatalogUtil.newTableMeta("TEXT", opts);

    Schema partSchema = new Schema();
    partSchema.addColumn("id", Type.INT4);
    partSchema.addColumn("name", Type.TEXT);

    PartitionMethodDesc partitionMethodDesc =
        new PartitionMethodDesc(DEFAULT_DATABASE_NAME, tableName,
            CatalogProtos.PartitionType.COLUMN, "id,name", partSchema);

    TableDesc desc =
        new TableDesc(tableName, schema, meta,
            new Path(CommonTestingUtil.getTestDir(), "addedtable").toUri());
    desc.setPartitionMethod(partitionMethodDesc);
    assertFalse(catalog.existsTable(tableName));
    catalog.createTable(desc);
    assertTrue(catalog.existsTable(tableName));

    TableDesc retrieved = catalog.getTableDesc(tableName);

    assertEquals(retrieved.getName(), tableName);
    assertEquals(retrieved.getPartitionMethod().getPartitionType(), CatalogProtos.PartitionType.COLUMN);
    assertEquals(retrieved.getPartitionMethod().getExpressionSchema().getColumn(0).getSimpleName(), "id");

    testAddPartition(tableName, "id=10/name=aaa");
    testAddPartition(tableName, "id=20/name=bbb");

    List<CatalogProtos.PartitionDescProto> partitions = catalog.getPartitions(DEFAULT_DATABASE_NAME, "addedtable");
    assertNotNull(partitions);
    assertEquals(partitions.size(), 2);

    testDropPartition(tableName, "id=10/name=aaa");
    testDropPartition(tableName, "id=20/name=bbb");

    partitions = catalog.getPartitions(DEFAULT_DATABASE_NAME, "addedtable");
    assertNotNull(partitions);
    assertEquals(partitions.size(), 0);

    catalog.dropTable(tableName);
    assertFalse(catalog.existsTable(tableName));
  }

  private void testAddPartition(String tableName, String partitionName) throws Exception {
    AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(tableName);
    alterTableDesc.setAlterTableType(AlterTableType.ADD_PARTITION);

    PartitionDesc partitionDesc = new PartitionDesc();
    partitionDesc.setPartitionName(partitionName);

    String[] partitionNames = partitionName.split("/");

    List<PartitionKeyProto> partitionKeyList = new ArrayList<PartitionKeyProto>();
    for(int i = 0; i < partitionNames.length; i++) {
      String columnName = partitionNames[i].split("=")[0];
      String partitionValue = partitionNames[i].split("=")[1];

      PartitionKeyProto.Builder builder = PartitionKeyProto.newBuilder();
      builder.setColumnName(partitionValue);
      builder.setPartitionValue(columnName);
      partitionKeyList.add(builder.build());
    }

    partitionDesc.setPartitionKeys(partitionKeyList);

    partitionDesc.setPath("hdfs://xxx.com/warehouse/" + partitionName);

    alterTableDesc.setPartitionDesc(partitionDesc);

    catalog.alterTable(alterTableDesc);

    CatalogProtos.PartitionDescProto resultDesc = catalog.getPartition(DEFAULT_DATABASE_NAME,
      "addedtable", partitionName);

    assertNotNull(resultDesc);
    assertEquals(resultDesc.getPartitionName(), partitionName);
    assertEquals(resultDesc.getPath(), "hdfs://xxx.com/warehouse/" + partitionName);

    assertEquals(resultDesc.getPartitionKeysCount(), 2);
  }


  private void testDropPartition(String tableName, String partitionName) throws Exception {
    AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(tableName);
    alterTableDesc.setAlterTableType(AlterTableType.DROP_PARTITION);

    PartitionDesc partitionDesc = new PartitionDesc();
    partitionDesc.setPartitionName(partitionName);

    alterTableDesc.setPartitionDesc(partitionDesc);

    catalog.alterTable(alterTableDesc);
  }

  @Test
  public void testAlterTableName () throws Exception {

    //CREATE_TABLE
    TableDesc tableRenameTestDesc = createMockupTable("default", "mycooltable") ;
    catalog.createTable(tableRenameTestDesc);

    //RENAME_TABLE
    catalog.alterTable(createMockAlterTableName());
    assertTrue(catalog.existsTable("default", "mynewcooltable"));

    //RENAME_COLUMN
    catalog.alterTable(createMockAlterTableRenameColumn());
    TableDesc columnRenameDesc = catalog.getTableDesc("default","mynewcooltable");
    assertTrue(columnRenameDesc.getSchema().containsByName("ren"+FieldName1));

    //ADD_COLUMN
    catalog.alterTable(createMockAlterTableAddColumn());
    TableDesc addColumnDesc = catalog.getTableDesc("default","mynewcooltable");
    assertTrue(addColumnDesc.getSchema().containsByName("mynewcol"));

    //SET_PROPERTY
    TableDesc setPropertyDesc = catalog.getTableDesc("default","mynewcooltable");
    KeyValueSet options = new KeyValueSet();
    options.set("timezone", "GMT+9");   // Seoul, Korea
    setPropertyDesc.setMeta(new TableMeta("TEXT", options));
    String prevTimeZone = setPropertyDesc.getMeta().getOption("timezone");
    String newTimeZone = "GMT-7";       // Silicon Valley, California
    catalog.alterTable(createMockAlterTableSetProperty(newTimeZone));
    setPropertyDesc = catalog.getTableDesc("default","mynewcooltable");
    assertNotEquals(prevTimeZone, setPropertyDesc.getMeta().getOption("timezone"));
    assertEquals(newTimeZone, setPropertyDesc.getMeta().getOption("timezone"));
  }

  private AlterTableDesc createMockAlterTableName(){
    AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName("default.mycooltable");
    alterTableDesc.setNewTableName("mynewcooltable");
    alterTableDesc.setAlterTableType(AlterTableType.RENAME_TABLE);
    return alterTableDesc;
  }

  private AlterTableDesc createMockAlterTableRenameColumn(){
    AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName("default.mynewcooltable");
    alterTableDesc.setColumnName(FieldName1);
    alterTableDesc.setNewColumnName("ren" + FieldName1);
    alterTableDesc.setAlterTableType(AlterTableType.RENAME_COLUMN);
    return alterTableDesc;
  }

  private AlterTableDesc createMockAlterTableAddColumn(){
    AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName("default.mynewcooltable");
    alterTableDesc.setAddColumn(new Column("mynewcol", Type.TEXT));
    alterTableDesc.setAlterTableType(AlterTableType.ADD_COLUMN);
    return alterTableDesc;
  }

  private AlterTableDesc createMockAlterTableSetProperty(String newTimeZone) {
    AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName("default.mynewcooltable");
    alterTableDesc.setProperty("timezone", newTimeZone);
    alterTableDesc.setAlterTableType(AlterTableType.SET_PROPERTY);
    return alterTableDesc;
  }

  public static class TestIntFunc extends Function {
    public TestIntFunc() {
      super(
          new Column [] {
              new Column("name", TajoDataTypes.Type.INT4),
              new Column("id", TajoDataTypes.Type.INT4)
          }
      );
    }
    public FunctionType getFunctionType() {
      return FunctionType.GENERAL;
    }
  }

  public static class TestFloatFunc extends Function {
    public TestFloatFunc() {
      super(
          new Column [] {
              new Column("name", TajoDataTypes.Type.FLOAT8),
              new Column("id", TajoDataTypes.Type.INT4)
          }
      );
    }
    public FunctionType getFunctionType() {
      return FunctionType.GENERAL;
    }
  }

  public static class TestAnyParamFunc extends Function {
    public TestAnyParamFunc() {
      super(
          new Column [] {
              new Column("name", Type.ANY),
          }
      );
    }
    public FunctionType getFunctionType() {
      return FunctionType.GENERAL;
    }
  }

  @Test
  public final void testFindIntFunc() throws Exception {
    assertFalse(catalog.containFunction("testint", FunctionType.GENERAL));
    FunctionDesc meta = new FunctionDesc("testint", TestIntFunc.class, FunctionType.GENERAL,
        CatalogUtil.newSimpleDataType(Type.INT4),
        CatalogUtil.newSimpleDataTypeArray(Type.INT4, Type.INT4));
    catalog.createFunction(meta);

    // UPGRADE TO INT4 SUCCESS==> LOOK AT SECOND PARAM BELOW
    FunctionDesc retrieved = catalog.getFunction("testint", CatalogUtil.newSimpleDataTypeArray(Type.INT4, Type.INT2));

    assertEquals(retrieved.getFunctionName(), "testint");
    assertEquals(retrieved.getParamTypes()[0], CatalogUtil.newSimpleDataType(Type.INT4));
    assertEquals(retrieved.getParamTypes()[1] , CatalogUtil.newSimpleDataType(Type.INT4));
  }

  @Test(expected=UndefinedFunctionException.class)
  public final void testFindIntInvalidFunc() throws Exception {
    assertFalse(catalog.containFunction("testintinvalid", FunctionType.GENERAL));
    FunctionDesc meta = new FunctionDesc("testintinvalid", TestIntFunc.class, FunctionType.GENERAL,
        CatalogUtil.newSimpleDataType(Type.INT4),
        CatalogUtil.newSimpleDataTypeArray(Type.INT4, Type.INT4));
    catalog.createFunction(meta);

    //UPGRADE TO INT8 WILL FAIL ==> LOOK AT SECOND PARAM BELOW
    catalog.getFunction("testintinvalid", CatalogUtil.newSimpleDataTypeArray(Type.INT4, Type.INT8));
  }

  @Test
  public final void testFindFloatFunc() throws Exception {
    assertFalse(catalog.containFunction("testfloat", FunctionType.GENERAL));
    FunctionDesc meta = new FunctionDesc("testfloat", TestFloatFunc.class, FunctionType.GENERAL,
        CatalogUtil.newSimpleDataType(Type.INT4),
        CatalogUtil.newSimpleDataTypeArray(Type.FLOAT8, Type.INT4));
    catalog.createFunction(meta);

    //UPGRADE TO FLOAT 8 SUCCESS==> LOOK AT FIRST PARAM BELOW
    FunctionDesc retrieved = catalog.getFunction("testfloat",
        CatalogUtil.newSimpleDataTypeArray(Type.FLOAT4, Type.INT4));

    assertEquals(retrieved.getFunctionName(), "testfloat");
    assertEquals(retrieved.getParamTypes()[0], CatalogUtil.newSimpleDataType(Type.FLOAT8));
    assertEquals(retrieved.getParamTypes()[1] , CatalogUtil.newSimpleDataType(Type.INT4));
  }

  @Test(expected=UndefinedFunctionException.class)
  public final void testFindFloatInvalidFunc() throws Exception {
    assertFalse(catalog.containFunction("testfloatinvalid", FunctionType.GENERAL));
    FunctionDesc meta = new FunctionDesc("testfloatinvalid", TestFloatFunc.class, FunctionType.GENERAL,
        CatalogUtil.newSimpleDataType(Type.INT4),
        CatalogUtil.newSimpleDataTypeArray(Type.FLOAT8, Type.INT4));
    catalog.createFunction(meta);

    // UPGRADE TO DECIMAL WILL FAIL ==> LOOK AT FIRST PARAM BELOW
    catalog.getFunction("testfloatinvalid", CatalogUtil.newSimpleDataTypeArray(Type.NUMERIC, Type.INT4));
  }

  @Test
  public final void testFindAnyTypeParamFunc() throws Exception {
    assertFalse(catalog.containFunction("testany", FunctionType.GENERAL));
    FunctionDesc meta = new FunctionDesc("testany", TestAnyParamFunc.class, FunctionType.GENERAL,
        CatalogUtil.newSimpleDataType(Type.INT4),
        CatalogUtil.newSimpleDataTypeArray(Type.ANY));
    catalog.createFunction(meta);

    FunctionDesc retrieved = catalog.getFunction("testany", CatalogUtil.newSimpleDataTypeArray(Type.INT1));
    assertEquals(retrieved.getFunctionName(), "testany");
    assertEquals(retrieved.getParamTypes()[0], CatalogUtil.newSimpleDataType(Type.ANY));

    retrieved = catalog.getFunction("testany", CatalogUtil.newSimpleDataTypeArray(Type.INT8));
    assertEquals(retrieved.getFunctionName(), "testany");
    assertEquals(retrieved.getParamTypes()[0], CatalogUtil.newSimpleDataType(Type.ANY));

    retrieved = catalog.getFunction("testany", CatalogUtil.newSimpleDataTypeArray(Type.FLOAT4));
    assertEquals(retrieved.getFunctionName(), "testany");
    assertEquals(retrieved.getParamTypes()[0], CatalogUtil.newSimpleDataType(Type.ANY));

    retrieved = catalog.getFunction("testany", CatalogUtil.newSimpleDataTypeArray(Type.TEXT));
    assertEquals(retrieved.getFunctionName(), "testany");
    assertEquals(retrieved.getParamTypes()[0], CatalogUtil.newSimpleDataType(Type.ANY));
  }
}
