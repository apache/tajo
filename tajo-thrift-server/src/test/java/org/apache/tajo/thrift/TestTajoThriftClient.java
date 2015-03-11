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

package org.apache.tajo.thrift;

import com.google.common.collect.Sets;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.thrift.client.TajoThriftClient;
import org.apache.tajo.thrift.client.TajoThriftResultSet;
import org.apache.tajo.thrift.generated.TGetQueryStatusResponse;
import org.apache.tajo.thrift.generated.TTableDesc;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class TestTajoThriftClient {
    private static TajoConf conf;
    private static TajoThriftClient client;
    private static Path testDir;

    @BeforeClass
    public static void setUp() throws Exception {
        String thriftServer = "localhost:26700";

        client = new TajoThriftClient(conf, thriftServer);
        testDir = CommonTestingUtil.getTestDir();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        client.close();
    }


    @Test
    public final void testCreateAndDropDatabases() throws Exception {
        int currentNum = client.getAllDatabaseNames().size();

        String prefix = CatalogUtil.normalizeIdentifier("testCreateDatabase_thrift_");
        for (int i = 0; i < 10; i++) {
            // test allDatabaseNames
            assertEquals(currentNum + i, client.getAllDatabaseNames().size());

            // test existence
            assertFalse(client.existDatabase(prefix + i));
            assertTrue(client.createDatabase(prefix + i));
            assertTrue(client.existDatabase(prefix + i));

            // test allDatabaseNames
            assertEquals(currentNum + i + 1, client.getAllDatabaseNames().size());
            assertTrue(client.getAllDatabaseNames().contains(prefix + i));
        }

        // test dropDatabase, existDatabase and getAllDatabaseNames()
        for (int i = 0; i < 10; i++) {
            assertTrue(client.existDatabase(prefix + i));
            assertTrue(client.getAllDatabaseNames().contains(prefix + i));
            assertTrue(client.dropDatabase(prefix + i));
            assertFalse(client.existDatabase(prefix + i));
            assertFalse(client.getAllDatabaseNames().contains(prefix + i));
        }

        assertEquals(currentNum, client.getAllDatabaseNames().size());
    }

    @Test
    public final void testCurrentDatabase() throws Exception {
        int currentNum = client.getAllDatabaseNames().size();
        assertEquals(TajoConstants.DEFAULT_DATABASE_NAME, client.getCurrentDatabase());

        String databaseName = CatalogUtil.normalizeIdentifier("thrift_testcurrentdatabase");
        assertTrue(client.createDatabase(databaseName));
        assertEquals(currentNum + 1, client.getAllDatabaseNames().size());
        assertEquals(TajoConstants.DEFAULT_DATABASE_NAME, client.getCurrentDatabase());
        assertTrue(client.selectDatabase(databaseName));
        assertEquals(databaseName, client.getCurrentDatabase());
        assertTrue(client.selectDatabase(TajoConstants.DEFAULT_DATABASE_NAME));
        assertTrue(client.dropDatabase(databaseName));

        assertEquals(currentNum, client.getAllDatabaseNames().size());
    }

    @Test
    public final void testSelectDatabaseToInvalidOne() throws Exception {
        int currentNum = client.getAllDatabaseNames().size();
        assertFalse(client.existDatabase("thrift_invaliddatabase"));

        try {
            assertTrue(client.selectDatabase("thrift_invaliddatabase"));
            assertFalse(true);
        } catch (Throwable t) {
            assertFalse(false);
        }

        assertEquals(currentNum, client.getAllDatabaseNames().size());
    }

    @Test
    public final void testDropCurrentDatabase() throws Exception {
        int currentNum = client.getAllDatabaseNames().size();
        String databaseName = CatalogUtil.normalizeIdentifier("thrift_testdropcurrentdatabase");
        assertTrue(client.createDatabase(databaseName));
        assertTrue(client.selectDatabase(databaseName));
        assertEquals(databaseName, client.getCurrentDatabase());

        try {
            client.dropDatabase(databaseName);
            assertFalse(true);
        } catch (Throwable t) {
            assertFalse(false);
        }

        assertTrue(client.selectDatabase(TajoConstants.DEFAULT_DATABASE_NAME));
        assertTrue(client.dropDatabase(databaseName));
        assertEquals(currentNum, client.getAllDatabaseNames().size());
    }

    @Test
    public final void testSessionVariables() throws Exception {
        String prefixName = "key_";
        String prefixValue = "val_";

        for (Map.Entry<String, String> entry : client.getAllSessionVariables().entrySet()) {
            client.unsetSessionVariable(entry.getKey());
        }

        for (int i = 0; i < 10; i++) {
            String key = prefixName + i;
            String val = prefixValue + i;

            // Basically,
            assertEquals(i + 4, client.getAllSessionVariables().size());
            assertFalse(client.getAllSessionVariables().containsKey(key));
            assertFalse(client.existSessionVariable(key));

            client.updateSessionVariable(key, val);

            assertEquals(i + 5, client.getAllSessionVariables().size());
            assertTrue(client.getAllSessionVariables().containsKey(key));
            assertTrue(client.existSessionVariable(key));
        }

        int totalSessionVarNum = client.getAllSessionVariables().size();

        for (int i = 0; i < 10; i++) {
            String key = prefixName + i;

            assertTrue(client.getAllSessionVariables().containsKey(key));
            assertTrue(client.existSessionVariable(key));

            client.unsetSessionVariable(key);

            assertFalse(client.getAllSessionVariables().containsKey(key));
            assertFalse(client.existSessionVariable(key));
        }

        assertEquals(totalSessionVarNum - 10, client.getAllSessionVariables().size());
    }

    @Test
    public final void testKillQuery() throws Exception {
        TGetQueryStatusResponse res = client.executeQuery("select sleep(2) from default.lineitem");
        while (true) {
            String state = client.getQueryStatus(res.getQueryId()).getState();
            if (QueryState.QUERY_RUNNING.name().equals(state)) {
                break;
            }
            Thread.sleep(100);
        }
        client.killQuery(res.getQueryId());
        Thread.sleep(5000);
        assertEquals(QueryState.QUERY_KILLED.name(), client.getQueryStatus(res.getQueryId()).getState());
    }



    @Test
    public final void testCreateAndDropTableByExecuteQuery() throws Exception {
        final String tableName = CatalogUtil.normalizeIdentifier("thrift_testCreateAndDropTableByExecuteQuery");

        assertFalse(client.existTable(tableName));

        String sql = "create table " + tableName + " (deptname text, score int4)";

        client.updateQuery(sql);
        assertTrue(client.existTable(tableName));

        Path tablePath = new Path(client.getTableDesc(tableName).getPath());
        FileSystem hdfs = tablePath.getFileSystem(conf);
        assertTrue(hdfs.exists(tablePath));

        client.updateQuery("drop table " + tableName);
        assertFalse(client.existTable(tableName));
        assertTrue(hdfs.exists(tablePath));
    }

    @Test
    public final void testCreateAndPurgeTableByExecuteQuery() throws Exception {
        final String tableName = CatalogUtil.normalizeIdentifier("thrift_testCreateAndPurgeTableByExecuteQuery");

        assertFalse(client.existTable(tableName));

        String sql = "create table " + tableName + " (deptname text, score int4)";

        client.updateQuery(sql);
        assertTrue(client.existTable(tableName));

        Path tablePath = new Path(client.getTableDesc(tableName).getPath());
        FileSystem hdfs = tablePath.getFileSystem(conf);
        assertTrue(hdfs.exists(tablePath));

        client.updateQuery("drop table " + tableName + " purge");
        assertFalse(client.existTable(tableName));
        assertFalse(hdfs.exists(tablePath));
    }



    @Test
    public final void testGetTableList() throws Exception {
        String tableName1 = "thrift_GetTableList1".toLowerCase();
        String tableName2 = "thrift_GetTableList2".toLowerCase();

        boolean result = client.existTable(tableName1);
        assertFalse(tableName1 + " exists", result);

        result = client.existTable(tableName2);
        assertFalse(result);
        client.updateQuery("create table thrift_GetTableList1 (age int, name text);");
        client.updateQuery("create table thrift_GetTableList2 (age int, name text);");

        assertTrue(client.existTable(tableName1));
        assertTrue(client.existTable(tableName2));

        Set<String> tables = Sets.newHashSet(client.getTableList(null));
        assertTrue(tables.contains(tableName1));
        assertTrue(tables.contains(tableName2));
    }

    @Test
    public final void testGetTableDesc() throws Exception {
        TTableDesc desc = client.getTableDesc("default.lineitem");
        assertNotNull(desc);
        assertEquals(CatalogUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, "lineitem"), desc.getTableName());
        assertTrue(desc.getStats().getNumBytes() > 0);
    }

    @Test
    public final void testFailCreateTablePartitionedOtherExceptColumn() throws Exception {
        final String tableName = "thrift_testFailCreateTablePartitionedOtherExceptColumn";

        assertFalse(client.existTable(tableName));

        String rangeSql = "create table " + tableName + " (deptname text, score int4)";
        rangeSql += "PARTITION BY RANGE (score)";
        rangeSql += "( PARTITION sub_part1 VALUES LESS THAN (2),";
        rangeSql += "PARTITION sub_part2 VALUES LESS THAN (5),";
        rangeSql += "PARTITION sub_part2 VALUES LESS THAN (MAXVALUE) )";

        assertFalse(client.updateQuery(rangeSql));

        String listSql = "create table " + tableName + " (deptname text, score int4)";
        listSql += "PARTITION BY LIST (deptname)";
        listSql += "( PARTITION sub_part1 VALUES('r&d', 'design'),";
        listSql += "PARTITION sub_part2 VALUES('sales', 'hr') )";

        assertFalse(client.updateQuery(listSql));

        String hashSql = "create table " + tableName + " (deptname text, score int4)";
        hashSql += "PARTITION BY HASH (deptname)";
        hashSql += "PARTITIONS 2";

        assertFalse(client.updateQuery(hashSql));
    }

    @Test
    public final void testCreateAndDropTablePartitionedColumnByExecuteQuery() throws Exception {
        final String tableName =
                CatalogUtil.normalizeIdentifier("thrift_testCreateAndDropTablePartitionedColumnByExecuteQuery");

        assertFalse(client.existTable(tableName));

        String sql = "create table " + tableName + " (deptname text, score int4)";
        sql += "PARTITION BY COLUMN (key1 text)";

        client.updateQuery(sql);
        assertTrue(client.existTable(tableName));

        Path tablePath = new Path(client.getTableDesc(tableName).getPath());
        FileSystem hdfs = tablePath.getFileSystem(conf);
        assertTrue(hdfs.exists(tablePath));

        client.updateQuery("drop table " + tableName + " purge");
        assertFalse(client.existTable(tableName));
        assertFalse(hdfs.exists(tablePath));
    }

    @Test
    public final void testSimpleQuery() throws Exception {
        TajoThriftResultSet resultSet = (TajoThriftResultSet) client.executeQueryAndGetResult(" select count(1),y from test2 group by y");
        assertNotNull(resultSet);


        assertEquals(3, resultSet.getTotalRow());
        resultSet.close();
    }

    @Test
    public final void testEvalQuery() throws Exception {
        ResultSet resultSet = client.executeQueryAndGetResult("select 1+1");
        assertNotNull(resultSet);

        String expected = "?plus\n" +
                "-------------------------------\n" +
                "2\n";

        assertEquals(expected, resultSetToString(resultSet));
        resultSet.close();
    }

    @Test
    public final void testGetFinishedQueryList() throws Exception {
        final String tableName = CatalogUtil.normalizeIdentifier("thrift_testGetFinishedQueryList");
        String sql = "create table " + tableName + " (deptname text, score int4)";

        client.updateQuery(sql);
        assertTrue(client.existTable(tableName));

        int numFinishedQueries = client.getFinishedQueryList().size();
        ResultSet resultSet = client.executeQueryAndGetResult("select * from " + tableName + " order by deptname");
        assertNotNull(resultSet);

        resultSet = client.executeQueryAndGetResult("select * from " + tableName + " order by deptname");
        assertNotNull(resultSet);
        assertEquals(numFinishedQueries + 2, client.getFinishedQueryList().size());

        resultSet.close();
    }


    /**
     * It transforms a ResultSet instance to rows represented as strings.
     *
     * @param resultSet ResultSet that contains a query result
     * @return String
     * @throws java.sql.SQLException
     */
    public static String resultSetToString(ResultSet resultSet) throws SQLException {
        StringBuilder sb = new StringBuilder();
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int numOfColumns = rsmd.getColumnCount();

        for (int i = 1; i <= numOfColumns; i++) {
            if (i > 1) sb.append(",");
            String columnName = rsmd.getColumnName(i);
            sb.append(columnName);
        }
        sb.append("\n-------------------------------\n");

        while (resultSet.next()) {
            for (int i = 1; i <= numOfColumns; i++) {
                if (i > 1) sb.append(",");
                String columnValue = resultSet.getString(i);
                if (resultSet.wasNull()) {
                    columnValue = "null";
                }
                sb.append(columnValue);
            }
            sb.append("\n");
        }
        return sb.toString();
    }
}
