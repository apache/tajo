package tajo.benchmark;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.util.FileUtil;

/**
 * @author Hyunsik Choi
 */
public class BenchmarkTPCH {
  private static final Log LOG = LogFactory.getLog(BenchmarkTPCH.class);
  public static final String[] Queries;
  private static final String BENCHMARK_DIR = "benchmark";

  static {
    Queries = new String[22];
    try {
      for (int i = 1; i <= Queries.length; i++) {
        Queries[i - 1] = FileUtil.readTextFile(BENCHMARK_DIR + "/q" + i
            + ".tql");
      }
    } catch (IOException e) {
      LOG.error(e);
    }
  }

  public static class SchemaTPCH {
    public static final Schema lineitem;
    public static final Schema customer;
    public static final Schema nation;
    public static final Schema part;
    public static final Schema region;
    public static final Schema orders;
    public static final Schema partsupp;
    public static final Schema supplier;

    static {
      customer = new Schema();
      customer.addColumn("c_custkey", DataType.INT);
      customer.addColumn("c_name", DataType.STRING);
      customer.addColumn("c_nationkey", DataType.INT);
      customer.addColumn("c_phone", DataType.STRING);
      customer.addColumn("c_acctbal", DataType.FLOAT);
      customer.addColumn("c_mktsegment", DataType.STRING);
      customer.addColumn("c_comment", DataType.STRING);

      nation = new Schema();
      nation.addColumn("n_nationkey", DataType.INT);
      nation.addColumn("n_name", DataType.STRING);
      nation.addColumn("n_regionkey", DataType.INT);
      nation.addColumn("n_comment", DataType.STRING);

      part = new Schema();
      part.addColumn("p_partkey", DataType.INT);
      part.addColumn("p_name", DataType.STRING);
      part.addColumn("p_mfgr", DataType.STRING);
      part.addColumn("p_brand", DataType.STRING);
      part.addColumn("p_type", DataType.STRING);
      part.addColumn("p_size", DataType.INT);
      part.addColumn("p_container", DataType.STRING);
      part.addColumn("p_retailprice", DataType.FLOAT);
      part.addColumn("p_comment", DataType.STRING);

      lineitem = new Schema();
      lineitem.addColumn("l_orderkey", DataType.INT);
      lineitem.addColumn("l_partkey", DataType.INT);
      lineitem.addColumn("l_suppkey", DataType.INT);
      lineitem.addColumn("l_linenumber", DataType.INT);
      lineitem.addColumn("l_quantity", DataType.FLOAT);
      lineitem.addColumn("l_extendedprice", DataType.FLOAT);
      lineitem.addColumn("l_discount", DataType.FLOAT);
      lineitem.addColumn("l_tax", DataType.FLOAT);
      lineitem.addColumn("l_returnflag", DataType.CHAR);
      lineitem.addColumn("l_linestatus", DataType.CHAR);
      lineitem.addColumn("l_shipdate", DataType.DATE);
      lineitem.addColumn("l_commitdate", DataType.DATE);
      lineitem.addColumn("l_receiptdate", DataType.DATE);
      lineitem.addColumn("l_shipinstruct", DataType.STRING);
      lineitem.addColumn("l_shipmode", DataType.STRING);
      lineitem.addColumn("l_comment", DataType.STRING);

      orders = new Schema();
      orders.addColumn("o_orderkey", DataType.INT);
      orders.addColumn("o_custkey", DataType.INT);
      orders.addColumn("o_orderstatus", DataType.STRING);
      orders.addColumn("o_totalprice", DataType.FLOAT);
      orders.addColumn("o_orderdate", DataType.DATE);
      orders.addColumn("o_orderpriority", DataType.STRING);
      orders.addColumn("o_clerk", DataType.STRING);
      orders.addColumn("o_shippriority", DataType.INT);
      orders.addColumn("o_comment", DataType.STRING);

      partsupp = new Schema();
      partsupp.addColumn("ps_partkey", DataType.INT);
      partsupp.addColumn("ps_suppkey", DataType.INT);
      partsupp.addColumn("ps_availqty", DataType.INT);
      partsupp.addColumn("ps_supplycost", DataType.FLOAT);
      partsupp.addColumn("ps_comment", DataType.STRING);

      region = new Schema();
      region.addColumn("r_regionkey", DataType.INT);
      region.addColumn("r_name", DataType.STRING);
      region.addColumn("r_comment", DataType.STRING);

      supplier = new Schema();
      supplier.addColumn("s_suppkey", DataType.INT);
      supplier.addColumn("s_name", DataType.STRING);
      supplier.addColumn("s_address", DataType.STRING);
      supplier.addColumn("s_nationkey", DataType.INT);
      supplier.addColumn("s_phone", DataType.STRING);
      supplier.addColumn("s_acctbal", DataType.FLOAT);
      supplier.addColumn("s_comment", DataType.STRING);
    }
  }
}
