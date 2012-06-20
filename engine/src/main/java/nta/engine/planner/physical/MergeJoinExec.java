package nta.engine.planner.physical;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import nta.catalog.Schema;
import nta.engine.SubqueryContext;
import nta.engine.exec.eval.EvalContext;
import nta.engine.parser.QueryBlock.SortSpec;
import nta.engine.planner.PlannerUtil;
import nta.engine.planner.Projector;
import nta.engine.planner.logical.JoinNode;
import nta.storage.FrameTuple;
import nta.storage.Tuple;
import nta.storage.VTuple;

public class MergeJoinExec extends PhysicalExec {
  // from logical plan
  private JoinNode joinNode;
  private Schema inSchema;
  private Schema outSchema;

  // sub operations
  private PhysicalExec outer;
  private PhysicalExec inner;

  // temporal tuples and states for nested loop join
  private FrameTuple frameTuple;
  private Tuple outerTuple = null;
  private Tuple innerTuple = null;
  private Tuple outTuple = null;
  private Tuple outerNext = null;

  private final List<Tuple> outerTupleSlots;
  private final List<Tuple> innerTupleSlots;
  private Iterator<Tuple> outerIterator;
  private Iterator<Tuple> innerIterator;

  private JoinTupleComparator joincomparator = null;
  private TupleComparator [] tupleComparator = null;

  private final static int INITIAL_TUPLE_SLOT = 10000;
  
  private boolean end = false;

  // projection
  private final Projector projector;
  private final EvalContext [] evalContexts;

  public MergeJoinExec(SubqueryContext ctx, JoinNode joinNode, PhysicalExec outer,
      PhysicalExec inner, SortSpec [] outerSortKey, SortSpec [] innerSortKey) {
    this.outer = outer;
    this.inner = inner;
    this.joinNode = joinNode;
    this.inSchema = joinNode.getInputSchema();
    this.outSchema = joinNode.getOutputSchema();

    this.outerTupleSlots = new ArrayList<Tuple>(INITIAL_TUPLE_SLOT);
    this.innerTupleSlots = new ArrayList<Tuple>(INITIAL_TUPLE_SLOT);
    SortSpec[][] sortSpecs = new SortSpec[2][];
    sortSpecs[0] = outerSortKey;
    sortSpecs[1] = innerSortKey;

    this.joincomparator = new JoinTupleComparator(outer.getSchema(),
        inner.getSchema(), sortSpecs);
    this.tupleComparator = PlannerUtil.getComparatorsFromJoinQual(
        joinNode.getJoinQual(), outer.getSchema(), inner.getSchema());
    this.outerIterator = outerTupleSlots.iterator();
    this.innerIterator = innerTupleSlots.iterator();
    
    // for projection
    this.projector = new Projector(inSchema, outSchema, joinNode.getTargets());
    this.evalContexts = projector.renew();

    // for join
    frameTuple = new FrameTuple();
    outTuple = new VTuple(outSchema.getColumnNum());
  }

  public PhysicalExec getInner(){
    return this.inner;
  }

  public PhysicalExec getOuter(){
    return this.outer;
  }

  public JoinNode getJoinNode(){
    return this.joinNode;
  }

  public Tuple next() throws IOException {
    Tuple previous;
    
    if (!outerIterator.hasNext() && !innerIterator.hasNext()) {
      if(end){
        return null;
      }
      
      if(outerTuple == null){
        outerTuple = outer.next();
      }
      if(innerTuple == null){
        innerTuple = inner.next();
      }
      
      outerTupleSlots.clear();
      innerTupleSlots.clear();
      
      int cmp;
      while ((cmp = joincomparator.compare(outerTuple, innerTuple)) != 0) {
        if (cmp > 0) {
          innerTuple = inner.next();
        } else if (cmp < 0) {
          outerTuple = outer.next();
        }
        if (innerTuple == null || outerTuple == null) {
          return null;
        }
      }
      
      previous = outerTuple;
      do {
        outerTupleSlots.add(outerTuple);
        outerTuple = outer.next();
        if (outerTuple == null) {
          end = true;
          break;
        }
      } while (tupleComparator[0].compare(previous, outerTuple) == 0);
      outerIterator = outerTupleSlots.iterator();
      outerNext = outerIterator.next();
      
      previous = innerTuple;
      do {
        innerTupleSlots.add(innerTuple);
        innerTuple = inner.next();
        if (innerTuple == null) {
          end = true;
          break;
        }
      } while (tupleComparator[1].compare(previous, innerTuple) == 0);
      innerIterator = innerTupleSlots.iterator();
    }
    
    if(!innerIterator.hasNext()){
      outerNext = outerIterator.next();
      innerIterator = innerTupleSlots.iterator();
    }
    
    frameTuple.set(outerNext, innerIterator.next());
    projector.eval(evalContexts, frameTuple);
    projector.terminate(evalContexts, outTuple);
    return outTuple;
  }

  @Override
  public Schema getSchema() {
    return outSchema;
  }

  @Override
  public void rescan() throws IOException {
    outer.rescan();
    inner.rescan();
    outerTupleSlots.clear();
    innerTupleSlots.clear();
    outerIterator = outerTupleSlots.iterator();
    innerIterator = innerTupleSlots.iterator();
  }
}
