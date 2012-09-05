package tajo.engine.planner.physical;

import tajo.SubqueryContext;
import tajo.engine.planner.logical.SortNode;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.io.IOException;
import java.util.*;

/**
 * @author Hyunsik Choi
 */
public class SortExec extends UnaryPhysicalExec {
  private final Comparator<Tuple> comparator;
  private List<Tuple> tupleSlots;
  private boolean sorted = false;
  private Iterator<Tuple> iterator;
  
  public SortExec(final SubqueryContext context,
                  SortNode plan, PhysicalExec child) {
    super(context, plan.getOutSchema(), plan.getInSchema(), child);

    this.comparator =
        new TupleComparator(inSchema, plan.getSortKeys());
  }

  public void init() throws IOException {
    super.init();
    this.tupleSlots = new ArrayList<Tuple>(1000);
  }

  @Override
  public Tuple next() throws IOException {

    if (!sorted) {
      Tuple tuple;
      while ((tuple = child.next()) != null) {
        tupleSlots.add(new VTuple(tuple));
      }
      
      Collections.sort(tupleSlots, this.comparator);
      this.iterator = tupleSlots.iterator();
      sorted = true;
    }
    
    if (iterator.hasNext()) {
      return this.iterator.next();
    } else {
      return null;
    }
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();
    this.iterator = tupleSlots.iterator();
    sorted = true;
  }
}
