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
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.storage.CSVFile;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

public class TPCH extends BenchmarkSet {
  private final Log LOG = LogFactory.getLog(TPCH.class);
  private final String BENCHMARK_DIR = "benchmark/tpch";

  public static final String LINEITEM = "lineitem";
  public static final String CUSTOMER = "customer";
  public static final String NATION = "nation";
  public static final String PART = "part";
  public static final String REGION = "region";
  public static final String ORDERS = "orders";
  public static final String PARTSUPP = "partsupp";
  public static final String SUPPLIER = "supplier";

  public static final Map<String, Long> tableVolumes = Maps.newHashMap();

  static {
    tableVolumes.put(LINEITEM, 759863287L);
    tableVolumes.put(CUSTOMER, 24346144L);
    tableVolumes.put(NATION, 2224L);
    tableVolumes.put(PART, 24135125L);
    tableVolumes.put(REGION, 389L);
    tableVolumes.put(ORDERS, 171952161L);
    tableVolumes.put(PARTSUPP, 118984616L);
    tableVolumes.put(SUPPLIER, 1409184L);
  }

  @Override
  public void loadSchemas() {
    Schema lineitem = new Schema()
        .addColumn("l_orderkey", Type.INT4) // 0
        .addColumn("l_partkey", Type.INT4) // 1
        .addColumn("l_suppkey", Type.INT4) // 2
        .addColumn("l_linenumber", Type.INT4) // 3
        .addColumn("l_quantity", Type.FLOAT8) // 4
        .addColumn("l_extendedprice", Type.FLOAT8) // 5
        .addColumn("l_discount", Type.FLOAT8) // 6
        .addColumn("l_tax", Type.FLOAT8) // 7
            // TODO - This is temporal solution. 8 and 9 are actually Char type.
        .addColumn("l_returnflag", Type.TEXT) // 8
        .addColumn("l_linestatus", Type.TEXT) // 9
            // TODO - This is temporal solution. 10,11, and 12 are actually Date type.
        .addColumn("l_shipdate", Type.TEXT) // 10
        .addColumn("l_commitdate", Type.TEXT) // 11
        .addColumn("l_receiptdate", Type.TEXT) // 12
        .addColumn("l_shipinstruct", Type.TEXT) // 13
        .addColumn("l_shipmode", Type.TEXT) // 14
        .addColumn("l_comment", Type.TEXT); // 15
    schemas.put(LINEITEM, lineitem);

    Schema customer = new Schema()
        .addColumn("c_custkey", Type.INT4) // 0
        .addColumn("c_name", Type.TEXT) // 1
        .addColumn("c_address", Type.TEXT) // 2
        .addColumn("c_nationkey", Type.INT4) // 3
        .addColumn("c_phone", Type.TEXT) // 4
        .addColumn("c_acctbal", Type.FLOAT8) // 5
        .addColumn("c_mktsegment", Type.TEXT) // 6
        .addColumn("c_comment", Type.TEXT); // 7
    schemas.put(CUSTOMER, customer);

    Schema nation = new Schema()
        .addColumn("n_nationkey", Type.INT4) // 0
        .addColumn("n_name", Type.TEXT) // 1
        .addColumn("n_regionkey", Type.INT4) // 2
        .addColumn("n_comment", Type.TEXT); // 3
    schemas.put(NATION, nation);

    Schema part = new Schema()
        .addColumn("p_partkey", Type.INT4) // 0
        .addColumn("p_name", Type.TEXT) // 1
        .addColumn("p_mfgr", Type.TEXT) // 2
        .addColumn("p_brand", Type.TEXT) // 3
        .addColumn("p_type", Type.TEXT) // 4
        .addColumn("p_size", Type.INT4) // 5
        .addColumn("p_container", Type.TEXT) // 6
        .addColumn("p_retailprice", Type.FLOAT8) // 7
        .addColumn("p_comment", Type.TEXT); // 8
    schemas.put(PART, part);

    Schema region = new Schema()
        .addColumn("r_regionkey", Type.INT4) // 0
        .addColumn("r_name", Type.TEXT) // 1
        .addColumn("r_comment", Type.TEXT); // 2
    schemas.put(REGION, region);

    Schema orders = new Schema()
        .addColumn("o_orderkey", Type.INT4) // 0
        .addColumn("o_custkey", Type.INT4) // 1
        .addColumn("o_orderstatus", Type.TEXT) // 2
        .addColumn("o_totalprice", Type.FLOAT8) // 3
            // TODO - This is temporal solution. o_orderdate is actually Date type.
        .addColumn("o_orderdate", Type.TEXT) // 4
        .addColumn("o_orderpriority", Type.TEXT) // 5
        .addColumn("o_clerk", Type.TEXT) // 6
        .addColumn("o_shippriority", Type.INT4) // 7
        .addColumn("o_comment", Type.TEXT); // 8
    schemas.put(ORDERS, orders);

    Schema partsupp = new Schema()
        .addColumn("ps_partkey", Type.INT4) // 0
        .addColumn("ps_suppkey", Type.INT4) // 1
        .addColumn("ps_availqty", Type.INT4) // 2
        .addColumn("ps_supplycost", Type.FLOAT8) // 3
        .addColumn("ps_comment", Type.TEXT); // 4
    schemas.put(PARTSUPP, partsupp);

    Schema supplier = new Schema()
        .addColumn("s_suppkey", Type.INT4) // 0
        .addColumn("s_name", Type.TEXT) // 1
        .addColumn("s_address", Type.TEXT) // 2
        .addColumn("s_nationkey", Type.INT4) // 3
        .addColumn("s_phone", Type.TEXT) // 4
        .addColumn("s_acctbal", Type.FLOAT8) // 5
        .addColumn("s_comment", Type.TEXT); // 6
    schemas.put(SUPPLIER, supplier);
  }

  public void loadOutSchema() {
    Schema q2 = new Schema()
        .addColumn("s_acctbal", Type.FLOAT8)
        .addColumn("s_name", Type.TEXT)
        .addColumn("n_name", Type.TEXT)
        .addColumn("p_partkey", Type.INT4)
        .addColumn("p_mfgr", Type.TEXT)
        .addColumn("s_address", Type.TEXT)
        .addColumn("s_phone", Type.TEXT)
        .addColumn("s_comment", Type.TEXT);
    outSchemas.put("q2", q2);
  }

  public void loadQueries() throws IOException {
    loadQueries(BENCHMARK_DIR);
  }

  public void loadTables() throws ServiceException {
    loadTable(LINEITEM);
    loadTable(CUSTOMER);
    loadTable(NATION);
    loadTable(PART);
    loadTable(REGION);
    loadTable(ORDERS);
    loadTable(PARTSUPP) ;
    loadTable(SUPPLIER);
  }

  private void loadTable(String tableName) throws ServiceException {
    TableMeta meta = CatalogUtil.newTableMeta(StoreType.CSV);
    meta.putOption(CSVFile.DELIMITER, "|");

    try {
      tajo.createExternalTable(tableName, getSchema(tableName), new Path(dataDir, tableName), meta);
    } catch (SQLException s) {
      throw new ServiceException(s);
    }
  }
}
