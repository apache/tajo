/**
 * 
 */
package tajo.engine.planner.physical;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import tajo.catalog.Schema;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.io.IOException;

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
   * @see SchemaObject#getSchema()
   */
  @Override
  public Schema getSchema() {
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see PhysicalExec#next()
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

  @Override
  public void rescan() throws IOException {
    // nothing to do
  }
}
