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

import org.apache.tajo.NamedTest;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.Int4Datum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.junit.Test;

import java.sql.ResultSet;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@NamedTest("TestJoinQuery")
public class TestJoinOnPartitionedTables extends TestJoinQuery {

  public TestJoinOnPartitionedTables(String joinOption) throws Exception {
    super(joinOption);
  }

  @Test
  public void testPartitionTableJoinSmallTable() throws Exception {
    executeDDL("partitioned_customer_ddl.sql", null);
    ResultSet res = executeFile("insert_into_customer.sql");
    res.close();

    res = executeQuery();
    assertResultSet(res);
    res.close();

    res = executeFile("selfJoinOfPartitionedTable.sql");
    assertResultSet(res, "selfJoinOfPartitionedTable.result");
    res.close();

    res = executeFile("testNoProjectionJoinQual.sql");
    assertResultSet(res, "testNoProjectionJoinQual.result");
    res.close();

    res = executeFile("testPartialFilterPushDown.sql");
    assertResultSet(res, "testPartialFilterPushDown.result");
    res.close();

    res = executeFile("testPartialFilterPushDownOuterJoin.sql");
    assertResultSet(res, "testPartialFilterPushDownOuterJoin.result");
    res.close();

    res = executeFile("testPartialFilterPushDownOuterJoin2.sql");
    assertResultSet(res, "testPartialFilterPushDownOuterJoin2.result");
    res.close();

    executeString("DROP TABLE customer_parts PURGE").close();
  }

  @Test
  public void testPartitionMultiplePartitionFilter() throws Exception {
    executeDDL("partitioned_customer_ddl.sql", null);
    ResultSet res = executeFile("insert_into_customer.sql");
    res.close();

    res = executeString(
        "select a.c_custkey, b.c_custkey from " +
            "  (select c_custkey, c_nationkey from customer_parts where c_nationkey < 0 " +
            "   union all " +
            "   select c_custkey, c_nationkey from customer_parts where c_nationkey < 0 " +
            ") a " +
            "left outer join customer_parts b " +
            "on a.c_custkey = b.c_custkey " +
            "and a.c_nationkey > 0"
    );

    String expected =
        "c_custkey,c_custkey\n" +
            "-------------------------------\n";
    assertEquals(expected, resultSetToString(res));
    res.close();

    executeString("DROP TABLE customer_parts PURGE").close();
  }

  @Test
  public void testFilterPushDownPartitionColumnCaseWhen() throws Exception {
    executeDDL("partitioned_customer_ddl.sql", null);
    ResultSet res = executeFile("insert_into_customer.sql");
    res.close();

    res = executeQuery();
    assertResultSet(res);
    res.close();

    executeString("DROP TABLE customer_parts PURGE").close();
  }

  @Test
  public void testMultiplePartitionedBroadcastDataFileWithZeroLength() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("nation_partitioned");
    ResultSet res = testBase.execute(
        "create table " + tableName + " (n_name text) partition by column(n_nationkey int4, n_regionkey int4) ");
    res.close();
    TajoTestingCluster cluster = testBase.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    res = executeString("insert overwrite into " + tableName
        + " select n_name, n_nationkey, n_regionkey from nation");
    res.close();

    addEmptyDataFile("nation_partitioned", true);

    res = executeQuery();

    assertResultSet(res);
    cleanupQuery(res);

    executeString("DROP TABLE nation_partitioned PURGE");
  }

  @Test
  public void testMultiplePartitionedBroadcastDataFileWithZeroLength2() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("nation_partitioned");
    ResultSet res = testBase.execute(
        "create table " + tableName + " (n_name text) partition by column(n_nationkey int4, n_regionkey int4) ");
    res.close();
    TajoTestingCluster cluster = testBase.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    res = executeString("insert overwrite into " + tableName
        + " select n_name, n_nationkey, n_regionkey from nation");
    res.close();

    addEmptyDataFile("nation_partitioned", true);

    res = executeQuery();

    assertResultSet(res);
    cleanupQuery(res);

    executeString("DROP TABLE nation_partitioned PURGE");
  }

  @Test
  public final void testCasebyCase1() throws Exception {
    // Left outer join with a small table and a large partition table which not matched any partition path.
    String tableName = CatalogUtil.normalizeIdentifier("largePartitionedTable");
    testBase.execute(
        "create table " + tableName + " (l_partkey int4, l_suppkey int4, l_linenumber int4, \n" +
            "l_quantity float8, l_extendedprice float8, l_discount float8, l_tax float8, \n" +
            "l_returnflag text, l_linestatus text, l_shipdate text, l_commitdate text, \n" +
            "l_receiptdate text, l_shipinstruct text, l_shipmode text, l_comment text) \n" +
            "partition by column(l_orderkey int4) ").close();
    TajoTestingCluster cluster = testBase.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    executeString("insert overwrite into " + tableName +
        " select l_partkey, l_suppkey, l_linenumber, \n" +
        " l_quantity, l_extendedprice, l_discount, l_tax, \n" +
        " l_returnflag, l_linestatus, l_shipdate, l_commitdate, \n" +
        " l_receiptdate, l_shipinstruct, l_shipmode, l_comment, l_orderkey from lineitem_large");

    ResultSet res = executeString(
        "select a.l_orderkey as key1, b.l_orderkey as key2 from lineitem as a " +
            "left outer join " + tableName + " b " +
            "on a.l_partkey = b.l_partkey and b.l_orderkey = 1000"
    );

    String expected = "key1,key2\n" +
        "-------------------------------\n" +
        "1,null\n" +
        "1,null\n" +
        "2,null\n" +
        "3,null\n" +
        "3,null\n";

    try {
      assertEquals(expected, resultSetToString(res));
    } finally {
      cleanupQuery(res);
    }
  }

  @Test
  public final void testBroadcastMultiColumnPartitionTable() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("testBroadcastMultiColumnPartitionTable");
    ResultSet res = testBase.execute(
        "create table " + tableName + " (col1 int4, col2 float4) partition by column(col3 text, col4 text) ");
    res.close();
    TajoTestingCluster cluster = testBase.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    res = executeString("insert overwrite into " + tableName
        + " select o_orderkey, o_totalprice, substr(o_orderdate, 6, 2), substr(o_orderdate, 1, 4) from orders");
    res.close();

    res = executeString(
        "select distinct a.col3 from " + tableName + " as a " +
            "left outer join lineitem_large b " +
            "on a.col1 = b.l_orderkey order by a.col3"
    );

    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testSelfJoin() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("paritioned_nation");
    ResultSet res = executeString(
        "create table " + tableName + " (n_name text,"
            + "  n_comment text, n_regionkey int8) USING csv "
            + "WITH ('csvfile.delimiter'='|')"
            + "PARTITION BY column(n_nationkey int8)");
    res.close();
    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    res = executeString(
        "insert overwrite into " + tableName
            + " select n_name, n_comment, n_regionkey, n_nationkey from nation");
    res.close();

    res = executeString(
        "select a.n_nationkey, a.n_name from nation a join nation b on a.n_nationkey = b.n_nationkey"
            + " where a.n_nationkey in (1)");
    String expected = resultSetToString(res);
    res.close();

    res = executeString(
        "select a.n_nationkey, a.n_name from " + tableName + " a join "+tableName +
            " b on a.n_nationkey = b.n_nationkey "
            + " where a.n_nationkey in (1)");
    String resultSetData = resultSetToString(res);
    res.close();

    assertEquals(expected, resultSetData);
  }

  @Test
  public final void testSelfJoin2() throws Exception {
    /*
     https://issues.apache.org/jira/browse/TAJO-1102
     See the following case.
     CREATE TABLE orders_partition
       (o_orderkey INT8, o_custkey INT8, o_totalprice FLOAT8, o_orderpriority TEXT,
          o_clerk TEXT, o_shippriority INT4, o_comment TEXT) USING CSV WITH ('csvfile.delimiter'='|')
       PARTITION BY COLUMN(o_orderdate TEXT, o_orderstatus TEXT);

     select a.o_orderstatus, count(*) as cnt
      from orders_partition a
      inner join orders_partition b
        on a.o_orderdate = b.o_orderdate
            and a.o_orderstatus = b.o_orderstatus
            and a.o_orderkey = b.o_orderkey
      where a.o_orderdate='1995-02-21'
        and a.o_orderstatus in ('F')
      group by a.o_orderstatus;

      Because of the where condition[where a.o_orderdate='1995-02-21 and a.o_orderstatus in ('F')],
        orders_partition table aliased a is small and broadcast target.
    */
    String tableName = CatalogUtil.normalizeIdentifier("partitioned_orders_large");
    ResultSet res = executeString(
        "create table " + tableName + " (o_orderkey INT8, o_custkey INT8, o_totalprice FLOAT8, o_orderpriority TEXT,\n" +
            "o_clerk TEXT, o_shippriority INT4, o_comment TEXT) USING CSV WITH ('csvfile.delimiter'='|')\n" +
            "PARTITION BY COLUMN(o_orderdate TEXT, o_orderstatus TEXT, o_orderkey_mod INT8)");
    res.close();
    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    res = executeString(
        "insert overwrite into " + tableName +
            " select o_orderkey, o_custkey, o_totalprice, " +
            " o_orderpriority, o_clerk, o_shippriority, o_comment, o_orderdate, o_orderstatus, o_orderkey % 10 " +
            " from orders_large ");
    res.close();

    res = executeString(
        "select a.o_orderdate, a.o_orderstatus, a.o_orderkey % 10 as o_orderkey_mod, a.o_totalprice " +
            "from orders_large a " +
            "join orders_large b on a.o_orderkey = b.o_orderkey " +
            "where a.o_orderdate = '1993-10-14' and a.o_orderstatus = 'F' and a.o_orderkey % 10 = 1" +
            " order by a.o_orderkey"
    );
    String expected = resultSetToString(res);
    res.close();

    res = executeString(
        "select a.o_orderdate, a.o_orderstatus, a.o_orderkey_mod, a.o_totalprice " +
            "from " + tableName +
            " a join "+ tableName + " b on a.o_orderkey = b.o_orderkey " +
            "where a.o_orderdate = '1993-10-14' and a.o_orderstatus = 'F' and a.o_orderkey_mod = 1 " +
            " order by a.o_orderkey"
    );
    String resultSetData = resultSetToString(res);
    res.close();

    assertEquals(expected, resultSetData);
  }

  @Test
  public final void testBroadcastPartitionTable() throws Exception {
    // If all tables participate in the BROADCAST JOIN, there is some missing data.
    executeDDL("customer_partition_ddl.sql", null);
    ResultSet res = executeFile("insert_into_customer_partition.sql");
    res.close();

    createMultiFile("nation", 2, new TupleCreator() {
      public Tuple createTuple(String[] columnDatas) {
        return new VTuple(new Datum[]{
            new Int4Datum(Integer.parseInt(columnDatas[0])),
            new TextDatum(columnDatas[1]),
            new Int4Datum(Integer.parseInt(columnDatas[2])),
            new TextDatum(columnDatas[3])
        });
      }
    });

    createMultiFile("orders", 1, new TupleCreator() {
      public Tuple createTuple(String[] columnDatas) {
        return new VTuple(new Datum[]{
            new Int4Datum(Integer.parseInt(columnDatas[0])),
            new Int4Datum(Integer.parseInt(columnDatas[1])),
            new TextDatum(columnDatas[2])
        });
      }
    });

    res = executeQuery();
    assertResultSet(res);
    res.close();

    executeString("DROP TABLE customer_broad_parts PURGE");
    executeString("DROP TABLE nation_multifile PURGE");
    executeString("DROP TABLE orders_multifile PURGE");
  }
}
