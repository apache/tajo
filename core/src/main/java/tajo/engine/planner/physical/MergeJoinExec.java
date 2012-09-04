package tajo.engine.planner.physical;

import com.google.common.base.Preconditions;
import tajo.SubqueryContext;
import tajo.catalog.Schema;
import tajo.engine.exec.eval.EvalContext;
import tajo.engine.exec.eval.EvalNode;
import tajo.engine.parser.QueryBlock;
import tajo.engine.planner.PlannerUtil;
import tajo.engine.planner.Projector;
import tajo.engine.planner.logical.JoinNode;
import tajo.storage.FrameTuple;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MergeJoinExec extends PhysicalExec {
  // from logical plan
  private JoinNode joinNode;
  private Schema inSchema;
  private Schema outSchema;
  private EvalNode joinQual;
  private EvalContext qualCtx;

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
      PhysicalExec inner, QueryBlock.SortSpec[] outerSortKey, QueryBlock.SortSpec[] innerSortKey) {
    Preconditions.checkArgument(joinNode.hasJoinQual(), "Sort-merge join is only used for the equi-join, " +
        "but there is no join condition");
    this.outer = outer;
    this.inner = inner;
    this.joinNode = joinNode;
    this.joinQual = joinNode.getJoinQual();
    this.qualCtx = this.joinQual.newContext();
    this.inSchema = joinNode.getInSchema();
    this.outSchema = joinNode.getOutSchema();

    this.outerTupleSlots = new ArrayList<Tuple>(INITIAL_TUPLE_SLOT);
    this.innerTupleSlots = new ArrayList<Tuple>(INITIAL_TUPLE_SLOT);
    QueryBlock.SortSpec[][] sortSpecs = new QueryBlock.SortSpec[2][];
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

    for (;;) {
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
      joinQual.eval(qualCtx, inSchema, frameTuple);
      if (joinQual.terminate(qualCtx).asBool()) {
        projector.eval(evalContexts, frameTuple);
        projector.terminate(evalContexts, outTuple);
        return outTuple;
      }
    }
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
