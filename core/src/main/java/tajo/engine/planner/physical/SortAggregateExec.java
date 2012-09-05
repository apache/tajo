package tajo.engine.planner.physical;

import tajo.SubqueryContext;
import tajo.catalog.Schema;
import tajo.engine.planner.Projector;
import tajo.engine.planner.logical.GroupbyNode;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.io.IOException;

/**
 * This is the sort-based Aggregation Operator.
 * 
 * @author Hyunsik Choi
 */
public class SortAggregateExec extends AggregationExec {
  private Tuple prevKey = null;
  private boolean finished = false;
  private final Projector projector;


  public SortAggregateExec(SubqueryContext context, GroupbyNode plan,
                           PhysicalExec child) throws IOException {
    super(context, plan, child);

    this.projector = new Projector(inSchema, outSchema, plan.getTargets());
  }

  @Override
  public Tuple next() throws IOException {
    Tuple curKey;
    Tuple tuple;
    Tuple finalTuple = null;
    while(!context.isStopped() && (tuple = child.next()) != null) {
      // build a key tuple
      curKey = new VTuple(keylist.length);
      for(int i = 0; i < keylist.length; i++) {
        curKey.put(i, tuple.get(keylist[i]));
      }

      if (prevKey == null || prevKey.equals(curKey)) {
        if (prevKey == null) {
          for(int i = 0; i < outSchema.getColumnNum(); i++) {
            evalContexts[i] = evals[i].newContext();
            evals[i].eval(evalContexts[i], inSchema, tuple);
          }
          prevKey = curKey;
        } else {
          // aggregate
          for (int idx : measureList) {
            evals[idx].eval(evalContexts[idx], inSchema, tuple);
          }
        }
      } else {
        // finalize aggregate and return
        finalTuple = new VTuple(outSchema.getColumnNum());
        for(int i = 0; i < outSchema.getColumnNum(); i++) {
          finalTuple.put(i, evals[i].terminate(evalContexts[i]));
        }

        for(int i = 0; i < outSchema.getColumnNum(); i++) {
          evalContexts[i] = evals[i].newContext();
          evals[i].eval(evalContexts[i], inSchema, tuple);
        }
        prevKey = curKey;
        return finalTuple;
      }
    } // while loop

    if (!finished) {
      finalTuple = new VTuple(outSchema.getColumnNum());
      for(int i = 0; i < outSchema.getColumnNum(); i++) {
        finalTuple.put(i, evals[i].terminate(evalContexts[i]));
      }
      finished = true;
    }
    return finalTuple;
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();

    prevKey = null;
    finished = false;
  }
}
