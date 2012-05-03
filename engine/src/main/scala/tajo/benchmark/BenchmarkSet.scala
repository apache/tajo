package tajo.benchmark

import nta.catalog.Schema
import tajo.client.TajoClient
import nta.engine.NConstants
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.net.NetUtils
import collection.immutable.HashMap
import java.io.File
import nta.util.FileUtil

/**
 * @author Hyunsik Choi
 */

abstract class BenchmarkSet(conf : org.apache.hadoop.conf.Configuration) {
  protected var tajo : TajoClient = _
  protected var schemas = HashMap[String, Schema]()

  if (System.getProperty(NConstants.MASTER_ADDRESS) != null) {
    tajo = new TajoClient(NetUtils.createSocketAddr(System.getProperty(NConstants.MASTER_ADDRESS)))
  } else {
    tajo = new TajoClient(conf)
  }

  protected var queries = HashMap[String, String]()

  protected def loadQueries(dir : String) {
    val queryDir = new File(dir);
    val files = queryDir.list();
    for (file <- files if file.endsWith(".tql")) {
      val last = file.indexOf(".tql")
      val name = file.substring(0, last)
      val query = FileUtil.readTextFile(queryDir + "/" + file)
      queries += (name -> query)
    }
  }

  def loadSchemas()

  def loadQueries()

  def loadTables()

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
    val start = System.currentTimeMillis();
    tajo.executeQuery(query)
    val end = System.currentTimeMillis();

    println("====================================")
    println("QueryName: " + queryName)
    println("Query: " + query)
    println("Processing Time: " + (end - start));
    println("====================================")
  }
}
