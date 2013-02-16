package tajo.benchmark;

import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import tajo.catalog.Schema;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableMeta;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.storage.CSVFile;

import java.io.IOException;

public class TPCH extends BenchmarkSet {
  private final Log LOG = LogFactory.getLog(TPCH.class);
  private final String BENCHMARK_DIR = "benchmark/tpch";

  public static String LINEITEM = "lineitem";
  public static String CUSTOMER = "customer";
  public static String NATION = "nation";
  public static String PART = "part";
  public static String REGION = "region";
  public static String ORDERS = "orders";
  public static String PARTSUPP = "partsupp";
  public static String SUPPLIER = "supplier";

  @Override
  public void loadSchemas() {
    Schema lineitem = new Schema()
        .addColumn("l_orderkey", DataType.LONG) // 0
        .addColumn("l_partkey", DataType.INT) // 1
        .addColumn("l_suppkey", DataType.INT) // 2
        .addColumn("l_linenumber", DataType.INT) // 3
        .addColumn("l_quantity", DataType.FLOAT) // 4
        .addColumn("l_extendedprice", DataType.FLOAT) // 5
        .addColumn("l_discount", DataType.FLOAT) // 6
        .addColumn("l_tax", DataType.FLOAT) // 7
            // TODO - This is temporal solution. 8 and 9 are actually Char type.
        .addColumn("l_returnflag", DataType.STRING) // 8
        .addColumn("l_linestatus", DataType.STRING) // 9
            // TODO - This is temporal solution. 10,11, and 12 are actually Date type.
        .addColumn("l_shipdate", DataType.STRING) // 10
        .addColumn("l_commitdate", DataType.STRING) // 11
        .addColumn("l_receiptdate", DataType.STRING) // 12
        .addColumn("l_shipinstruct", DataType.STRING) // 13
        .addColumn("l_shipmode", DataType.STRING) // 14
        .addColumn("l_comment", DataType.STRING); // 15
    schemas.put(LINEITEM, lineitem);

    Schema customer = new Schema()
        .addColumn("c_custkey", DataType.INT) // 0
        .addColumn("c_name", DataType.STRING) // 1
        .addColumn("c_address", DataType.STRING) // 2
        .addColumn("c_nationkey", DataType.INT) // 3
        .addColumn("c_phone", DataType.STRING) // 4
        .addColumn("c_acctbal", DataType.FLOAT) // 5
        .addColumn("c_mktsegment", DataType.STRING) // 6
        .addColumn("c_comment", DataType.STRING); // 7
    schemas.put(CUSTOMER, customer);

    Schema nation = new Schema()
        .addColumn("n_nationkey", DataType.INT) // 0
        .addColumn("n_name", DataType.STRING) // 1
        .addColumn("n_regionkey", DataType.INT) // 2
        .addColumn("n_comment", DataType.STRING); // 3
    schemas.put(NATION, nation);

    Schema part = new Schema()
        .addColumn("p_partkey", DataType.INT) // 0
        .addColumn("p_name", DataType.STRING) // 1
        .addColumn("p_mfgr", DataType.STRING) // 2
        .addColumn("p_brand", DataType.STRING) // 3
        .addColumn("p_type", DataType.STRING) // 4
        .addColumn("p_size", DataType.INT) // 5
        .addColumn("p_container", DataType.STRING) // 6
        .addColumn("p_retailprice", DataType.FLOAT) // 7
        .addColumn("p_comment", DataType.STRING); // 8
    schemas.put(PART, part);

    Schema region = new Schema()
        .addColumn("r_regionkey", DataType.INT) // 0
        .addColumn("r_name", DataType.STRING) // 1
        .addColumn("r_comment", DataType.STRING); // 2
    schemas.put(REGION, region);

    Schema orders = new Schema()
        .addColumn("o_orderkey", DataType.INT) // 0
        .addColumn("o_custkey", DataType.INT) // 1
        .addColumn("o_orderstatus", DataType.STRING) // 2
        .addColumn("o_totalprice", DataType.FLOAT) // 3
            // TODO - This is temporal solution. o_orderdate is actually Date type.
        .addColumn("o_orderdate", DataType.STRING) // 4
        .addColumn("o_orderpriority", DataType.STRING) // 5
        .addColumn("o_clerk", DataType.STRING) // 6
        .addColumn("o_shippriority", DataType.INT) // 7
        .addColumn("o_comment", DataType.STRING); // 8
    schemas.put(ORDERS, orders);

    Schema partsupp = new Schema()
        .addColumn("ps_partkey", DataType.INT) // 0
        .addColumn("ps_suppkey", DataType.INT) // 1
        .addColumn("ps_availqty", DataType.INT) // 2
        .addColumn("ps_supplycost", DataType.FLOAT) // 3
        .addColumn("ps_comment", DataType.STRING); // 4
    schemas.put(PARTSUPP, partsupp);

    Schema supplier = new Schema()
        .addColumn("s_suppkey", DataType.INT) // 0
        .addColumn("s_name", DataType.STRING) // 1
        .addColumn("s_address", DataType.STRING) // 2
        .addColumn("s_nationkey", DataType.INT) // 3
        .addColumn("s_phone", DataType.STRING) // 4
        .addColumn("s_acctbal", DataType.FLOAT) // 5
        .addColumn("s_comment", DataType.STRING); // 6
    schemas.put(SUPPLIER, supplier);
  }

  public void loadOutSchema() {
    Schema q2 = new Schema()
        .addColumn("s_acctbal", DataType.FLOAT)
        .addColumn("s_name", DataType.STRING)
        .addColumn("n_name", DataType.STRING)
        .addColumn("p_partkey", DataType.INT)
        .addColumn("p_mfgr", DataType.STRING)
        .addColumn("s_address", DataType.STRING)
        .addColumn("s_phone", DataType.STRING)
        .addColumn("s_comment", DataType.STRING);
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
    TableMeta meta = TCatUtil.newTableMeta(getSchema(tableName), StoreType.CSV);
    meta.putOption(CSVFile.DELIMITER, "|");
    tajo.createTable(tableName, new Path(dataDir, tableName), meta);
  }
}
