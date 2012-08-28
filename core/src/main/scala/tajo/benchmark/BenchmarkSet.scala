package tajo.benchmark

import tajo.client.TajoClient
import org.apache.hadoop.net.NetUtils
import collection.immutable.HashMap
import java.io.File
import tajo.catalog.{TConstants, Schema}
import tajo.util.FileUtil
import tajo.catalog.store.MemStore
import tajo.conf.TajoConf
import tajo.conf.TajoConf.ConfVars

/**
 * @author Hyunsik Choi
 */

abstract class BenchmarkSet() {
  protected var tajo : TajoClient = _
  protected var schemas = HashMap[String, Schema]()
  protected var outSchemas = HashMap[String, Schema]()
  protected var datadir : String = _

  def init(conf : TajoConf, _datadir : String) {
    this.datadir = _datadir
    if (System.getProperty(ConfVars.MASTER_ADDRESS.varname) != null) {
      tajo = new TajoClient(NetUtils.createSocketAddr(
        System.getProperty(ConfVars.MASTER_ADDRESS.varname)))
    } else {
      conf.set(TConstants.STORE_CLASS, classOf[MemStore].getCanonicalName)
      tajo = new TajoClient(conf)
    }
  }

  protected var queries = HashMap[String, String]()

  protected def loadQueries(dir : String) {
    val queryDir = new File(dir)
    val files = queryDir.list()
    for (file <- files if file.endsWith(".tql")) {
      val last = file.indexOf(".tql")
      val name = file.substring(0, last)
      val query = FileUtil.readTextFile(new File(queryDir + "/" + file))
      queries += (name -> query)
    }
  }

  def loadSchemas()

  def loadOutSchema()

  def loadQueries()

  def loadTables()

  def getTableNames : Array[String] = {
    schemas.keySet.toArray
  }

  def getQuery(queryName : String) : String = {
    queries(queryName)
  }

  def getSchema(name : String) : Schema = {
    schemas(name)
  }

  def getSchemas() : Iterator[Schema] = {
    schemas.valuesIterator
  }

  def getOutSchema(name : String) : Schema = {
    outSchemas(name)
  }

  def perform(queryName : String) {
    val query = queries(queryName)
    if (query == null) {
      throw new IllegalArgumentException("#{queryName} does not exists")
    }
    val start = System.currentTimeMillis()
    tajo.executeQuery(query)
    val end = System.currentTimeMillis()

    println("====================================")
    println("QueryName: " + queryName)
    println("Query: " + query)
    println("Processing Time: " + (end - start))
    println("====================================")
  }
}
