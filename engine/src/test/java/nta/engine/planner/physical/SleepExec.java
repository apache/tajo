/**
 * 
 */
package nta.engine.planner.physical;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import nta.catalog.Schema;
import nta.storage.Tuple;
import nta.storage.VTuple;

/**
 * @author Hyunsik Choi
 */
public class SleepExec extends PhysicalExec {
  private final Log LOG = LogFactory.getLog(SleepExec.class);
  private final String msg;

  public SleepExec(String msg) {
    this.msg = msg;
  }

  /*
   * (non-Javadoc)
   * 
   * @see nta.engine.SchemaObject#getSchema()
   */
  @Override
  public Schema getSchema() {
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see nta.engine.planner.physical.PhysicalExec#next()
   */
  @Override
  public Tuple next() throws IOException {
    try {
      Thread.sleep(1000);
      LOG.info(msg);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return new VTuple(1);
  }
}
