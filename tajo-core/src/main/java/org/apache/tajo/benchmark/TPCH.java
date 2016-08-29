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

package org.apache.tajo.benchmark;

import com.google.common.collect.Maps;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaBuilder;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.TajoException;

import java.io.IOException;
import java.util.Map;

public class TPCH extends BenchmarkSet {
  private final String BENCHMARK_DIR = "benchmark/tpch";

  public static final String LINEITEM = "lineitem";
  public static final String CUSTOMER = "customer";
  public static final String CUSTOMER_PARTS = "customer_parts";
  public static final String NATION = "nation";
  public static final String PART = "part";
  public static final String REGION = "region";
  public static final String ORDERS = "orders";
  public static final String PARTSUPP = "partsupp";
  public static final String SUPPLIER = "supplier";
  public static final String SUPPLIER_COPY = "small_supplier";
  public static final String EMPTY_ORDERS = "empty_orders";


  public static final Map<String, Long> tableVolumes = Maps.newHashMap();

  static {
    tableVolumes.put(LINEITEM, 759863287L);
    tableVolumes.put(CUSTOMER, 24346144L);
    tableVolumes.put(CUSTOMER_PARTS, 707L);
    tableVolumes.put(NATION, 2224L);
    tableVolumes.put(PART, 24135125L);
    tableVolumes.put(REGION, 389L);
    tableVolumes.put(ORDERS, 171952161L);
    tableVolumes.put(PARTSUPP, 118984616L);
    tableVolumes.put(SUPPLIER, 1409184L);
    tableVolumes.put(SUPPLIER_COPY, 5120L);
    tableVolumes.put(EMPTY_ORDERS, 0L);

  }

  @Override
  public void loadSchemas() {
    schemas.put(LINEITEM, SchemaBuilder.builder()
        .add("l_orderkey", Type.INT4) // 0
        .add("l_partkey", Type.INT4) // 1
        .add("l_suppkey", Type.INT4) // 2
        .add("l_linenumber", Type.INT4) // 3
        .add("l_quantity", Type.FLOAT8) // 4
        .add("l_extendedprice", Type.FLOAT8) // 5
        .add("l_discount", Type.FLOAT8) // 6
        .add("l_tax", Type.FLOAT8) // 7
            // TODO - This is temporal solution. 8 and 9 are actually Char type.
        .add("l_returnflag", Type.TEXT) // 8
        .add("l_linestatus", Type.TEXT) // 9
            // TODO - This is temporal solution. 10,11, and 12 are actually Date type.
        .add("l_shipdate", Type.TEXT) // 10
        .add("l_commitdate", Type.TEXT) // 11
        .add("l_receiptdate", Type.TEXT) // 12
        .add("l_shipinstruct", Type.TEXT) // 13
        .add("l_shipmode", Type.TEXT) // 14
        .add("l_comment", Type.TEXT) // 15
        .build());

    schemas.put(CUSTOMER, SchemaBuilder.builder()
        .add("c_custkey", Type.INT4) // 0
        .add("c_name", Type.TEXT) // 1
        .add("c_address", Type.TEXT) // 2
        .add("c_nationkey", Type.INT4) // 3
        .add("c_phone", Type.TEXT) // 4
        .add("c_acctbal", Type.FLOAT8) // 5
        .add("c_mktsegment", Type.TEXT) // 6
        .add("c_comment", Type.TEXT) // 7
        .build());


    schemas.put(CUSTOMER_PARTS, SchemaBuilder.builder()
        .add("c_custkey", Type.INT4) // 0
        .add("c_name", Type.TEXT) // 1
        .add("c_address", Type.TEXT) // 2
        .add("c_phone", Type.TEXT) // 3
        .add("c_acctbal", Type.FLOAT8) // 4
        .add("c_mktsegment", Type.TEXT) // 5
        .add("c_comment", Type.TEXT) // 6
        .build());


    schemas.put(NATION, SchemaBuilder.builder()
        .add("n_nationkey", Type.INT4) // 0
        .add("n_name", Type.TEXT) // 1
        .add("n_regionkey", Type.INT4) // 2
        .add("n_comment", Type.TEXT) // 3
        .build());


    schemas.put(PART, SchemaBuilder.builder()
        .add("p_partkey", Type.INT4) // 0
        .add("p_name", Type.TEXT) // 1
        .add("p_mfgr", Type.TEXT) // 2
        .add("p_brand", Type.TEXT) // 3
        .add("p_type", Type.TEXT) // 4
        .add("p_size", Type.INT4) // 5
        .add("p_container", Type.TEXT) // 6
        .add("p_retailprice", Type.FLOAT8) // 7
        .add("p_comment", Type.TEXT) // 8
        .build());


    schemas.put(REGION, SchemaBuilder.builder()
        .add("r_regionkey", Type.INT4) // 0
        .add("r_name", Type.TEXT) // 1
        .add("r_comment", Type.TEXT) // 2
        .build());


    Schema orders = SchemaBuilder.builder()
        .add("o_orderkey", Type.INT4) // 0
        .add("o_custkey", Type.INT4) // 1
        .add("o_orderstatus", Type.TEXT) // 2
        .add("o_totalprice", Type.FLOAT8) // 3
        // TODO - This is temporal solution. o_orderdate is actually Date type.
        .add("o_orderdate", Type.TEXT) // 4
        .add("o_orderpriority", Type.TEXT) // 5
        .add("o_clerk", Type.TEXT) // 6
        .add("o_shippriority", Type.INT4) // 7
        .add("o_comment", Type.TEXT) // 8
        .build();
    schemas.put(ORDERS, orders);
    schemas.put(EMPTY_ORDERS, orders);


    schemas.put(PARTSUPP, SchemaBuilder.builder()
        .add("ps_partkey", Type.INT4) // 0
        .add("ps_suppkey", Type.INT4) // 1
        .add("ps_availqty", Type.INT4) // 2
        .add("ps_supplycost", Type.FLOAT8) // 3
        .add("ps_comment", Type.TEXT) // 4
        .build());


    Schema supplier = SchemaBuilder.builder()
        .add("s_suppkey", Type.INT4) // 0
        .add("s_name", Type.TEXT) // 1
        .add("s_address", Type.TEXT) // 2
        .add("s_nationkey", Type.INT4) // 3
        .add("s_phone", Type.TEXT) // 4
        .add("s_acctbal", Type.FLOAT8) // 5
        .add("s_comment", Type.TEXT) // 6
        .build();
    schemas.put(SUPPLIER, supplier);
    schemas.put(SUPPLIER_COPY, supplier);
  }

  public void loadOutSchema() {
    Schema q2 = SchemaBuilder.builder()
        .add("s_acctbal", Type.FLOAT8)
        .add("s_name", Type.TEXT)
        .add("n_name", Type.TEXT)
        .add("p_partkey", Type.INT4)
        .add("p_mfgr", Type.TEXT)
        .add("s_address", Type.TEXT)
        .add("s_phone", Type.TEXT)
        .add("s_comment", Type.TEXT)
        .build();
    outSchemas.put("q2", q2);
  }

  public void loadQueries() throws IOException {
    loadQueries(BENCHMARK_DIR);
  }

  public void loadTables() throws TajoException {
    loadTable(LINEITEM);
    loadTable(CUSTOMER);
    loadTable(CUSTOMER_PARTS);
    loadTable(NATION);
    loadTable(PART);
    loadTable(REGION);
    loadTable(ORDERS);
    loadTable(PARTSUPP) ;
    loadTable(SUPPLIER);
    loadTable(SUPPLIER_COPY);
    loadTable(EMPTY_ORDERS);

  }

  public void loadTable(String tableName) throws TajoException {
    TableMeta meta = CatalogUtil.newTableMeta(BuiltinStorages.TEXT, new TajoConf());

    PartitionMethodDesc partitionMethodDesc = null;
    if (tableName.equals(CUSTOMER_PARTS)) {
      Schema expressionSchema = SchemaBuilder.builder()
      .add("c_nationkey", TajoDataTypes.Type.INT4).build();
      partitionMethodDesc = new PartitionMethodDesc(
          tajo.getCurrentDatabase(),
          CUSTOMER_PARTS,
          CatalogProtos.PartitionType.COLUMN,
          "c_nationkey",
          expressionSchema);
    }

    tajo.createExternalTable(tableName, getSchema(tableName),
        new Path(dataDir, tableName).toUri(), meta, partitionMethodDesc);
  }
}
