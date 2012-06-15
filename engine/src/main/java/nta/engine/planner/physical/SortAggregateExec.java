package nta.engine.planner.physical;

import nta.catalog.Schema;
import nta.engine.SubqueryContext;
import nta.engine.planner.logical.GroupbyNode;
import nta.storage.Tuple;
import nta.storage.VTuple;

import java.io.IOException;

/**
 * This is the sort-based Aggregation Operator.
 * 
 * @author Hyunsik Choi
 */
public class SortAggregateExec extends AggregationExec {
  private Tuple prevKey = null;
  private Tuple newAggTuple = null;
  private Tuple aggTuple = null;
  private boolean finished = false;

  public SortAggregateExec(SubqueryContext ctx, GroupbyNode annotation,
                           PhysicalExec subOp) throws IOException {
    super(ctx, annotation, subOp);
    this.newAggTuple = new VTuple(outputSchema.getColumnNum());
  }

  @Override
  public Tuple next() throws IOException {
    Tuple curKey;
    Tuple tuple;
    Tuple finalTuple = null;
    while(!ctx.isStopped() && (tuple = subOp.next()) != null) {
      // build a key tuple
      curKey = new VTuple(keylist.length);
      for(int i = 0; i < keylist.length; i++) {
        curKey.put(i, tuple.get(keylist[i]));
      }

      if (prevKey == null || prevKey.equals(curKey)) {
        if (prevKey == null) {
          for(int i = 0; i < outputSchema.getColumnNum(); i++) {
            evalContexts[i] = evals[i].newContext();
            evals[i].eval(evalContexts[i], inputSchema, tuple);
          }
          prevKey = curKey;
        } else {
          // aggregate
          for (int idx : measureList) {
            evals[idx].eval(evalContexts[idx], inputSchema, tuple);
          }
        }
      } else {
        // finalize aggregate and return
        finalTuple = new VTuple(outputSchema.getColumnNum());
        for(int i = 0; i < outputSchema.getColumnNum(); i++) {
          finalTuple.put(i, evals[i].terminate(evalContexts[i]));
        }

        for(int i = 0; i < outputSchema.getColumnNum(); i++) {
          evalContexts[i] = evals[i].newContext();
          evals[i].eval(evalContexts[i], inputSchema, tuple);
        }
        prevKey = curKey;
        return finalTuple;
      }
    }

    if (!finished) {
      finalTuple = new VTuple(outputSchema.getColumnNum());
      for(int i = 0; i < outputSchema.getColumnNum(); i++) {
        finalTuple.put(i, evals[i].terminate(evalContexts[i]));
      }
      finished = true;
    }
    return finalTuple;
  }

  @Override
  public Schema getSchema() {
    return outputSchema;
  }

  @Override
  public void rescan() throws IOException {
    prevKey = null;
    finished = false;
    subOp.rescan();
  }
}
