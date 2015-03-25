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
import org.apache.tajo.*;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.thrift.client.TajoThriftClient;
import org.apache.tajo.thrift.client.TajoThriftResultSet;
import org.apache.tajo.thrift.generated.TGetQueryStatusResponse;
import org.apache.tajo.thrift.generated.TTableDesc;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestTajoThriftClient {
    private static TajoTestingCluster cluster;
    private static TajoConf conf;
    private static TajoThriftClient client;
    private static Path testDir;

    @BeforeClass
    public static void setUp() throws Exception {
        cluster = TpchTestBase.getInstance().getTestingCluster();
        conf = new TajoConf(cluster.getConfiguration());
        String thriftServer = cluster.getThriftServer().getContext().getServerName();

        client = new TajoThriftClient(conf, thriftServer);
        testDir = CommonTestingUtil.getTestDir();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        client.close();
    }

    private static Path writeTmpTable(String tableName) throws IOException {
        Path tablePath = StorageUtil.concatPath(testDir, tableName);
        BackendTestingUtil.writeTmpTable(conf, tablePath);
        return tablePath;
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

        for(Map.Entry<String, String> entry: client.getAllSessionVariables().entrySet()) {
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
    public final void testUpdateQuery() throws Exception {
        final String tableName = CatalogUtil.normalizeIdentifier("thrift_testUpdateQuery");
        Path tablePath = writeTmpTable(tableName);

        assertFalse(client.existTable(tableName));
        String sql =
                "create external table " + tableName + " (deptname text, score integer) "
                        + "using csv location '" + tablePath + "'";
        client.updateQuery(sql);
        assertTrue(client.existTable(tableName));
        client.dropTable(tableName);
        assertFalse(client.existTable(tableName));
    }

    @Test
    public final void testCreateAndDropTableByExecuteQuery() throws Exception {
        TajoConf conf = cluster.getConfiguration();
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
        TajoConf conf = cluster.getConfiguration();
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
    public final void testDDLByExecuteQuery() throws Exception {
        final String tableName = CatalogUtil.normalizeIdentifier("thrift_testDDLByExecuteQuery");
        Path tablePath = writeTmpTable(tableName);

        boolean result = client.existTable(tableName);
        assertFalse(tableName + " exists", result);
        String sql =
                "create external table " + tableName + " (deptname text, score int4) "
                        + "using csv location '" + tablePath + "'";
        client.executeQueryAndGetResult(sql);
        assertTrue(client.existTable(tableName));
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
        TajoConf conf = cluster.getConfiguration();
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
        TajoConf conf = cluster.getConfiguration();
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
        TajoThriftResultSet resultSet = (TajoThriftResultSet)client.executeQueryAndGetResult("select * from default.nation");
        assertNotNull(resultSet);

        String expected = "n_nationkey,n_name,n_regionkey,n_comment\n" +
                "-------------------------------\n" +
                "0,ALGERIA,0, haggle. carefully final deposits detect slyly agai\n" +
                "1,ARGENTINA,1,al foxes promise slyly according to the regular accounts. bold requests alon\n" +
                "2,BRAZIL,1,y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special \n" +
                "3,CANADA,1,eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold\n" +
                "4,EGYPT,4,y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d\n" +
                "5,ETHIOPIA,0,ven packages wake quickly. regu\n" +
                "6,FRANCE,3,refully final requests. regular, ironi\n" +
                "7,GERMANY,3,l platelets. regular accounts x-ray: unusual, regular acco\n" +
                "8,INDIA,2,ss excuses cajole slyly across the packages. deposits print aroun\n" +
                "9,INDONESIA,2, slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull\n" +
                "10,IRAN,4,efully alongside of the slyly final dependencies. \n" +
                "11,IRAQ,4,nic deposits boost atop the quickly final requests? quickly regula\n" +
                "12,JAPAN,2,ously. final, express gifts cajole a\n" +
                "13,JORDAN,4,ic deposits are blithely about the carefully regular pa\n" +
                "14,KENYA,0, pending excuses haggle furiously deposits. pending, express pinto beans wake fluffily past t\n" +
                "15,MOROCCO,0,rns. blithely bold courts among the closely regular packages use furiously bold platelets?\n" +
                "16,MOZAMBIQUE,0,s. ironic, unusual asymptotes wake blithely r\n" +
                "17,PERU,1,platelets. blithely pending dependencies use fluffily across the even pinto beans. carefully silent accoun\n" +
                "18,CHINA,2,c dependencies. furiously express notornis sleep slyly regular accounts. ideas sleep. depos\n" +
                "19,ROMANIA,3,ular asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic account\n" +
                "20,SAUDI ARABIA,4,ts. silent requests haggle. closely express packages sleep across the blithely\n" +
                "21,VIETNAM,2,hely enticingly express accounts. even, final \n" +
                "22,RUSSIA,3, requests against the platelets use never according to the quickly regular pint\n" +
                "23,UNITED KINGDOM,3,eans boost carefully special requests. accounts are. carefull\n" +
                "24,UNITED STATES,1,y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be\n";

        assertEquals(expected, resultSetToString(resultSet));
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
     * @throws SQLException
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
