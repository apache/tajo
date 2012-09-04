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


  public SortAggregateExec(SubqueryContext ctx, GroupbyNode groupbyNode,
                           PhysicalExec subOp) throws IOException {
    super(ctx, groupbyNode, subOp);
    this.projector = new Projector(inputSchema, outputSchema, groupbyNode.getTargets());
  }

  @Override
  public Tuple next() throws IOException {
    Tuple curKey;
    Tuple tuple;
    Tuple finalTuple = null;
    while(!ctx.isStopped() && (tuple = child.next()) != null) {
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
    } // while loop

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
    child.rescan();
  }
}
