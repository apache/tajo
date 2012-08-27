package tajo.engine.planner.physical;

import tajo.catalog.Schema;
import tajo.engine.planner.logical.SortNode;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.io.IOException;
import java.util.*;

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
  private SortNode annotation;
  
  public SortExec(SortNode annotation, PhysicalExec subOp) {
    this.subOp = subOp;
    this.annotation = annotation;
    
    this.inputSchema = annotation.getInputSchema();
    this.outputSchema = annotation.getOutputSchema();
    
    this.comparator =
        new TupleComparator(inputSchema, annotation.getSortKeys());
    this.tupleSlots = new ArrayList<Tuple>(1000);
  }

  public PhysicalExec getSubOp(){
    return this.subOp;
  }

  public SortNode getSortNode(){
    return this.annotation;
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
