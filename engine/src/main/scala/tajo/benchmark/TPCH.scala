package tajo.benchmark

import nta.util.FileUtil
import tajo.client.TajoClient
import org.apache.hadoop.net.NetUtils
import collection.immutable.{Nil, Map, HashMap}
import nta.conf.NtaConf
import org.apache.commons.logging.{LogFactory, Log}
import nta.engine.NConstants
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import nta.catalog._
import proto.CatalogProtos.{DataType, StoreType}
import nta.storage.CSVFile2
;

class TPCH (conf : Configuration, datadir : String) extends BenchmarkSet(conf) {
  private final val LOG : Log  = LogFactory.getLog(classOf[TPCH])
  private final val BENCHMARK_DIR: String = "benchmark/tpch"
  private val _datadir = datadir;

  override def loadSchemas() {
    schemas += ("lineitem" -> (new Schema)
      .addColumn("l_orderkey", DataType.INT)  // 0
      .addColumn("l_partkey", DataType.INT) // 1
      .addColumn("l_suppkey", DataType.INT) // 2
      .addColumn("l_linenumber", DataType.INT) // 3
      .addColumn("l_quantity", DataType.FLOAT) // 4
      .addColumn("l_extendedprice", DataType.FLOAT) // 5
      .addColumn("l_discount", DataType.FLOAT) // 6
      .addColumn("l_tax", DataType.FLOAT) // 7
      .addColumn("l_returnflag", DataType.CHAR) // 8
      .addColumn("l_linestatus", DataType.CHAR) // 9
      .addColumn("l_shipdate", DataType.DATE) // 10
      .addColumn("l_commitdate", DataType.DATE) // 11
      .addColumn("l_receiptdate", DataType.DATE) // 12
      .addColumn("l_shipinstruct", DataType.STRING) // 13
      .addColumn("l_shipmode", DataType.STRING) // 14
      .addColumn("l_comment", DataType.STRING)) // 15

    schemas += ("customer" -> (new Schema)
      .addColumn("c_custkey", DataType.INT)
      .addColumn("c_name", DataType.STRING)
      .addColumn("c_nationkey", DataType.INT)
      .addColumn("c_phone", DataType.STRING)
      .addColumn("c_acctbal", DataType.FLOAT)
      .addColumn("c_mktsegment", DataType.STRING)
      .addColumn("c_comment", DataType.STRING))

    schemas += ("nation" -> (new Schema)
      .addColumn("n_nationkey", DataType.INT)
      .addColumn("n_name", DataType.STRING)
      .addColumn("n_regionkey", DataType.INT)
      .addColumn("n_comment", DataType.STRING))

    schemas += ("part" -> (new Schema)
      .addColumn("p_partkey", DataType.INT)
      .addColumn("p_name", DataType.STRING)
      .addColumn("p_mfgr", DataType.STRING)
      .addColumn("p_brand", DataType.STRING)
      .addColumn("p_type", DataType.STRING)
      .addColumn("p_size", DataType.INT)
      .addColumn("p_container", DataType.STRING)
      .addColumn("p_retailprice", DataType.FLOAT)
      .addColumn("p_comment", DataType.STRING))

    schemas += ("region" -> (new Schema)
      .addColumn("r_regionkey", DataType.INT)
      .addColumn("r_name", DataType.STRING)
      .addColumn("r_comment", DataType.STRING))

    schemas += ("orders" -> (new Schema)
      .addColumn("o_orderkey", DataType.INT)
      .addColumn("o_custkey", DataType.INT)
      .addColumn("o_orderstatus", DataType.STRING)
      .addColumn("o_totalprice", DataType.FLOAT)
      .addColumn("o_orderdate", DataType.DATE)
      .addColumn("o_orderpriority", DataType.STRING)
      .addColumn("o_clerk", DataType.STRING)
      .addColumn("o_shippriority", DataType.INT)
      .addColumn("o_comment", DataType.STRING))

    schemas += ("partsupp" -> (new Schema)
      .addColumn("ps_partkey", DataType.INT)
      .addColumn("ps_suppkey", DataType.INT)
      .addColumn("ps_availqty", DataType.INT)
      .addColumn("ps_supplycost", DataType.FLOAT)
      .addColumn("ps_comment", DataType.STRING))

    schemas += ("supplier" -> (new Schema)
      .addColumn("s_suppkey", DataType.INT)
      .addColumn("s_name", DataType.STRING)
      .addColumn("s_address", DataType.STRING)
      .addColumn("s_nationkey", DataType.INT)
      .addColumn("s_phone", DataType.STRING)
      .addColumn("s_acctbal", DataType.FLOAT)
      .addColumn("s_comment", DataType.STRING))
  }

  override def loadQueries() {
    loadQueries(BENCHMARK_DIR)
  }

  override def loadTables() {
    loadTable("lineitem")
    loadTable("customer")
    loadTable("nation")
    loadTable("part")
    loadTable("region")
    loadTable("orders")
    loadTable("partsupp")
    loadTable("supplier")
  }

  private def loadTable(tbName : String) {
    val meta = TCatUtil.newTableMeta(schemas(tbName), StoreType.CSV)
    meta.putOption(CSVFile2.DELIMITER, "|")
    tajo.createTable(tbName, new Path(datadir, tbName), meta)
  }
}
