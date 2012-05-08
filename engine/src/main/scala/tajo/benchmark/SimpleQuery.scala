package tajo.benchmark

import org.apache.commons.logging.{LogFactory, Log}
import org.apache.hadoop.conf.Configuration
import nta.util.FileUtil

/**
 * @author Hyunsik Choi
 */

class SimpleQuery () extends TPCH {
  private final val LOG : Log  = LogFactory.getLog(classOf[TPCH])
  private final val BENCHMARK_DIR: String = "benchmark/simple"

  override def loadQueries() {
    loadQueries(BENCHMARK_DIR)
  }
}
