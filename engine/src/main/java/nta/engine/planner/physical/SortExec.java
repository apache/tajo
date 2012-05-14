package nta.engine.planner.physical;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import nta.catalog.Schema;
import nta.engine.planner.logical.SortNode;
import nta.storage.Tuple;
import nta.storage.VTuple;

/**
 * @author Hyunsik Choi
 */
public class SortExec extends PhysicalExec {  
  private PhysicalExec subOp;
  private final Schema inputSchema;
  private final Schema outputSchema;
  
  private final Comparator<Tuple> comparator;
  private final List<Tuple> tupleSlots;  
  private boolean sorted = false;
  private Iterator<Tuple> iterator;

  public SortExec(SortNode annotation, PhysicalExec subOp) {
    this.subOp = subOp;
    
    this.inputSchema = annotation.getInputSchema();
    this.outputSchema = annotation.getOutputSchema();
    
    this.comparator =
        new TupleComparator(inputSchema, annotation.getSortKeys(), null);    
    this.tupleSlots = new ArrayList<Tuple>(1000);
  }
  
  @Override
  public Schema getSchema() {
    return this.outputSchema;
  }

  @Override
  public Tuple next() throws IOException {
    if (!sorted) {
      Tuple tuple;
      while ((tuple = subOp.next()) != null) {
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
    this.iterator = tupleSlots.iterator();
    sorted = true;
  }
}
