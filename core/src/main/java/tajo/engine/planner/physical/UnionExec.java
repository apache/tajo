/**
 * 
 */
package tajo.engine.planner.physical;

import tajo.SubqueryContext;
import tajo.catalog.Schema;
import tajo.engine.query.exception.InvalidQueryException;
import tajo.storage.Tuple;

import java.io.IOException;

/**
 * @author Hyunsik Choi
 *
 */
public class UnionExec extends BinaryPhysicalExec {
  private boolean nextOuter = true;
  private Tuple tuple;

  public UnionExec(SubqueryContext context, PhysicalExec outer, PhysicalExec inner) {
    super(context, outer.getSchema(), inner.getSchema(), outer, inner);
    if (!outer.getSchema().equals(inner.getSchema())) {
      throw new InvalidQueryException(
          "The both schemas are not same");
    }
  }

  @Override
  public Tuple next() throws IOException {
    if (nextOuter) {
      tuple = outerChild.next();
      if (tuple == null) {
       nextOuter = false; 
      } else {
        return tuple;
      }
    }
    
    return innerChild.next();
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();

    nextOuter = true;
  }
}
