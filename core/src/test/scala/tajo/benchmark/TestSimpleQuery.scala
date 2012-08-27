package tajo.benchmark

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfterAll, FunSuite}

@RunWith(classOf[JUnitRunner])
class TestSimpleQuery extends FunSuite with BeforeAndAfterAll {
  var benchmark : SimpleQuery = _
  var conf : Configuration = _

  override def beforeAll() {
    benchmark = new SimpleQuery()
    benchmark.loadSchemas()
    benchmark.loadQueries()
    // the test of load table
    //benchmark.loadTables()
  }

  override def afterAll() {
  }

  test("Schema Load Test") {
    assert(benchmark.getSchema("lineitem") != null)
    assert(benchmark.getSchema("customer") != null)
    assert(benchmark.getSchema("nation") != null)
    assert(benchmark.getSchema("part") != null)
    assert(benchmark.getSchema("region") != null)
    assert(benchmark.getSchema("orders") != null)
    assert(benchmark.getSchema("partsupp") != null)
    assert(benchmark.getSchema("supplier") != null)
  }

  test("Query Load Test") {
    assert(benchmark.getQuery("selection1") != null)
  }

  /*
  test("Table Load Test Passed") {
    val client = new TajoClient(conf)
    assert(client.existTable("lineitem.tbl"))
    assert(client.existTable("customer"))
    assert(client.existTable("nation"))
    assert(client.existTable("part"))
    assert(client.existTable("region"))
    assert(client.existTable("orders"))
    assert(client.existTable("partsupp"))
    assert(client.existTable("supplier"))
  }
  */
}

