package tajo.benchmark

import tajo.client.TajoClient
import nta.engine.NConstants
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.net.NetUtils
import collection.immutable.HashMap
import java.io.File
import nta.util.FileUtil
import nta.catalog.{TConstants, Schema}
import nta.catalog.store.MemStore

/**
 * @author Hyunsik Choi
 */

abstract class BenchmarkSet() {
  protected var tajo : TajoClient = _
  protected var schemas = HashMap[String, Schema]()
  protected var datadir : String = _

  def init(conf : org.apache.hadoop.conf.Configuration, _datadir : String) {
    this.datadir = _datadir
    if (System.getProperty(NConstants.MASTER_ADDRESS) != null) {
      tajo = new TajoClient(NetUtils.createSocketAddr(System.getProperty(NConstants.MASTER_ADDRESS)))
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
