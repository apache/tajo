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

package org.apache.tajo.engine.planner;

import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.OpType;
import org.apache.tajo.algebra.Selection;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos.GetPartitionsWithDirectSQLRequest;
import org.apache.tajo.catalog.proto.CatalogProtos.PartitionType;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.engine.function.FunctionLoader;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.expr.AlgebraicUtil;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.exprrewrite.EvalTreeOptimizer;
import org.apache.tajo.plan.nameresolver.NameResolvingMode;
import org.apache.tajo.plan.rewrite.rules.PartitionedTableRewriter;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.KeyValueSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.assertEquals;

public class TestPartitionedTableRewriter {

  static TajoTestingCluster util;
  static CatalogService catalog = null;
  static SQLAnalyzer analyzer;
  static LogicalPlanner planner;
  static QueryContext defaultContext;

  private static final String TABLE_NAME = "partition_table";

  public static final String [] QUERIES = {
    "select score from partition_table where score > 7", // 0
    "select score from partition_table where 10 * 2 > score * 10", // 1
    "select score from partition_table where score < 10 and 4 < score", // 2
    "select score from partition_table where score = 10 and age > 5", // 3
  };

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    catalog = util.getMiniCatalogCluster().getCatalog();
    for (FunctionDesc funcDesc : FunctionLoader.findLegacyFunctions()) {
      catalog.createFunction(funcDesc);
    }
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, "hdfs://localhost:1234/warehouse");
    catalog.createDatabase(TajoConstants.DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);

    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4)
      .addColumn("name", TajoDataTypes.Type.TEXT);

    String tableName = CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, TABLE_NAME);
    KeyValueSet opts = new KeyValueSet();
    opts.set("file.delimiter", ",");
    TableMeta meta = CatalogUtil.newTableMeta("TEXT", opts);

    Schema partSchema = new Schema();
    partSchema.addColumn("age", TajoDataTypes.Type.INT4);
    partSchema.addColumn("score", TajoDataTypes.Type.FLOAT8);

    PartitionMethodDesc partitionMethodDesc =
      new PartitionMethodDesc(DEFAULT_DATABASE_NAME, tableName, PartitionType.COLUMN, "age,score", partSchema);

    TableDesc desc = new TableDesc(
      CatalogUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, TABLE_NAME), schema, meta,
      CommonTestingUtil.getTestDir().toUri());

    desc.setPartitionMethod(partitionMethodDesc);

    catalog.createTable(desc);

    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog, TablespaceManager.getInstance());

    defaultContext = LocalTajoTestingUtility.createDummyContext(util.getConfiguration());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  public static EvalNode getRootSelection(String query) throws TajoException {
    Expr block = analyzer.parse(query);
    LogicalPlan plan = planner.createPlan(defaultContext, block);

    LogicalPlanner.PlanContext context = new LogicalPlanner.PlanContext(defaultContext, plan, plan.getRootBlock(),
      new EvalTreeOptimizer(), true);

    Selection selection = plan.getRootBlock().getSingletonExpr(OpType.Filter);
    return planner.getExprAnnotator().createEvalNode(context, selection.getQual(),
      NameResolvingMode.RELS_AND_SUBEXPRS);
  }

  @Test
  public final void testQuery1() throws TajoException {
    EvalNode node = getRootSelection(QUERIES[0]);
    EvalNode [] filters = AlgebraicUtil.toDisjunctiveNormalFormArray(node);
    assertEquals(1, filters.length);

    assertEquals("default.partition_table.score (FLOAT8) > 7.0", filters[0].toString());

    Schema partitionColumns = new Schema();
    partitionColumns.addColumn(DEFAULT_DATABASE_NAME + "." + TABLE_NAME + ".age", TajoDataTypes.Type.INT4);
    partitionColumns.addColumn(DEFAULT_DATABASE_NAME + "." + TABLE_NAME + ".score", TajoDataTypes.Type.FLOAT8);

    GetPartitionsWithDirectSQLRequest request = PartitionedTableRewriter.buildDirectSQLForRDBMS
      (DEFAULT_DATABASE_NAME, TABLE_NAME, partitionColumns, filters);

    assertEquals(request.getDatabaseName(), DEFAULT_DATABASE_NAME);
    assertEquals(request.getTableName(), TABLE_NAME);

    String directSQL = "\n SELECT A.PARTITION_ID, A.PARTITION_NAME, A.PATH FROM PARTITIONS A \n WHERE A.TID = ? \n " +
      "AND A.PARTITION_ID IN (\n   SELECT T1.PARTITION_ID FROM PARTITION_KEYS T1 " +
      "\n   JOIN PARTITION_KEYS T2 ON T1.TID=T2.TID AND T1.PARTITION_ID = T2.PARTITION_ID " +
      "AND T2.TID = ? AND ( T2.COLUMN_NAME = \'score\' AND T2.PARTITION_VALUE > ? )" +
      "\n   WHERE T1.TID = ? AND ( T1.COLUMN_NAME = \'age\' AND T1.PARTITION_VALUE IS NOT NULL )\n )";

    assertEquals(request.getDirectSQL(), directSQL);

    assertEquals(request.getFiltersCount(), 2);
    assertEquals(request.getFilters(0).getColumnName(), "score");
    assertEquals(request.getFilters(0).getParameterValue(0), "7.0");
    assertEquals(request.getFilters(1).getColumnName(), "age");
    assertEquals(request.getFilters(1).getParameterValueCount(), 0);
 }


  @Test
  public final void testQuery2() throws TajoException {
    EvalNode node = getRootSelection(QUERIES[1]);
    EvalNode [] filters = AlgebraicUtil.toDisjunctiveNormalFormArray(node);
    assertEquals(1, filters.length);

    assertEquals("20.0 > default.partition_table.score (FLOAT8) * 10.0", filters[0].toString());

    Schema partitionColumns = new Schema();
    partitionColumns.addColumn(DEFAULT_DATABASE_NAME + "." + TABLE_NAME + ".age", TajoDataTypes.Type.INT4);
    partitionColumns.addColumn(DEFAULT_DATABASE_NAME + "." + TABLE_NAME + ".score", TajoDataTypes.Type.FLOAT8);

    GetPartitionsWithDirectSQLRequest request = PartitionedTableRewriter.buildDirectSQLForRDBMS
      (DEFAULT_DATABASE_NAME, TABLE_NAME, partitionColumns, filters);

    assertEquals(request.getDatabaseName(), DEFAULT_DATABASE_NAME);
    assertEquals(request.getTableName(), TABLE_NAME);

    String directSQL = "\n SELECT A.PARTITION_ID, A.PARTITION_NAME, A.PATH FROM PARTITIONS A \n WHERE A.TID = ? \n " +
      "AND A.PARTITION_ID IN (\n   SELECT T1.PARTITION_ID FROM PARTITION_KEYS T1 " +
      "\n   JOIN PARTITION_KEYS T2 ON T1.TID=T2.TID AND T1.PARTITION_ID = T2.PARTITION_ID " +
      "AND T2.TID = ? AND ? ) > ( T2.COLUMN_NAME = \'score\' AND T2.PARTITION_VALUE * ? )" +
      "\n   WHERE T1.TID = ? AND ( T1.COLUMN_NAME = \'age\' AND T1.PARTITION_VALUE IS NOT NULL )\n )";

    assertEquals(request.getDirectSQL(), directSQL);

    assertEquals(request.getFiltersCount(), 2);
    assertEquals(request.getFilters(0).getColumnName(), "score");
    assertEquals(request.getFilters(0).getParameterValue(0), "20.0");
    assertEquals(request.getFilters(0).getParameterValue(1), "10.0");
    assertEquals(request.getFilters(1).getColumnName(), "age");
    assertEquals(request.getFilters(1).getParameterValueCount(), 0);
  }

  @Test
  public final void testQuery3() throws TajoException {
    EvalNode node = getRootSelection(QUERIES[2]);
    EvalNode [] filters = AlgebraicUtil.toDisjunctiveNormalFormArray(node);
    assertEquals(1, filters.length);

    assertEquals("default.partition_table.score (FLOAT8) < 10.0 AND 4.0 < default.partition_table.score (FLOAT8)"
      , filters[0].toString());

    Schema partitionColumns = new Schema();
    partitionColumns.addColumn(DEFAULT_DATABASE_NAME + "." + TABLE_NAME + ".age", TajoDataTypes.Type.INT4);
    partitionColumns.addColumn(DEFAULT_DATABASE_NAME + "." + TABLE_NAME + ".score", TajoDataTypes.Type.FLOAT8);

    GetPartitionsWithDirectSQLRequest request = PartitionedTableRewriter.buildDirectSQLForRDBMS
      (DEFAULT_DATABASE_NAME, TABLE_NAME, partitionColumns, filters);

    assertEquals(request.getDatabaseName(), DEFAULT_DATABASE_NAME);
    assertEquals(request.getTableName(), TABLE_NAME);

    String directSQL = "\n SELECT A.PARTITION_ID, A.PARTITION_NAME, A.PATH FROM PARTITIONS A \n WHERE A.TID = ? \n " +
      "AND A.PARTITION_ID IN (\n   SELECT T1.PARTITION_ID FROM PARTITION_KEYS T1 " +
      "\n   JOIN PARTITION_KEYS T2 ON T1.TID=T2.TID AND T1.PARTITION_ID = T2.PARTITION_ID " +
      "AND T2.TID = ? AND ( T2.COLUMN_NAME = \'score\' AND T2.PARTITION_VALUE < ? ) " +
      "AND ? ) < ( T2.COLUMN_NAME = \'score\' AND T2.PARTITION_VALUE" +
      "\n   WHERE T1.TID = ? AND ( T1.COLUMN_NAME = \'age\' AND T1.PARTITION_VALUE IS NOT NULL )\n )";

    assertEquals(request.getDirectSQL(), directSQL);

    assertEquals(request.getFiltersCount(), 2);
    assertEquals(request.getFilters(0).getColumnName(), "score");
    assertEquals(request.getFilters(0).getParameterValue(0), "10.0");
    assertEquals(request.getFilters(0).getParameterValue(1), "4.0");
    assertEquals(request.getFilters(1).getColumnName(), "age");
    assertEquals(request.getFilters(1).getParameterValueCount(), 0);
  }

  @Test
  public final void testQuery4() throws TajoException {
    EvalNode node = getRootSelection(QUERIES[3]);
    EvalNode [] filters = AlgebraicUtil.toConjunctiveNormalFormArray(node);
    assertEquals(2, filters.length);

    assertEquals("default.partition_table.score (FLOAT8) = 10.0", filters[0].toString());
    assertEquals("default.partition_table.age (INT4) > 5", filters[1].toString());

    Schema partitionColumns = new Schema();
    partitionColumns.addColumn(DEFAULT_DATABASE_NAME + "." + TABLE_NAME + ".age", TajoDataTypes.Type.INT4);
    partitionColumns.addColumn(DEFAULT_DATABASE_NAME + "." + TABLE_NAME + ".score", TajoDataTypes.Type.FLOAT8);

    GetPartitionsWithDirectSQLRequest request = PartitionedTableRewriter.buildDirectSQLForRDBMS
      (DEFAULT_DATABASE_NAME, TABLE_NAME, partitionColumns, filters);

    assertEquals(request.getDatabaseName(), DEFAULT_DATABASE_NAME);
    assertEquals(request.getTableName(), TABLE_NAME);

    String directSQL = "\n SELECT A.PARTITION_ID, A.PARTITION_NAME, A.PATH FROM PARTITIONS A \n WHERE A.TID = ? \n " +
      "AND A.PARTITION_ID IN (\n   SELECT T1.PARTITION_ID FROM PARTITION_KEYS T1 " +
      "\n   JOIN PARTITION_KEYS T2 ON T1.TID=T2.TID AND T1.PARTITION_ID = T2.PARTITION_ID " +
      "AND T2.TID = ? AND ( T2.COLUMN_NAME = \'score\' AND T2.PARTITION_VALUE = ? )" +
      "\n   WHERE T1.TID = ? AND ( T1.COLUMN_NAME = \'age\' AND T1.PARTITION_VALUE > ? )\n )";

    assertEquals(request.getDirectSQL(), directSQL);

    assertEquals(request.getFiltersCount(), 2);
    assertEquals(request.getFilters(0).getColumnName(), "score");
    assertEquals(request.getFilters(0).getParameterValue(0), "10.0");
    assertEquals(request.getFilters(1).getColumnName(), "age");
    assertEquals(request.getFilters(1).getParameterValue(0), "5");
  }
}