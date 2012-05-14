package nta.engine.planner.physical;

import nta.catalog.Schema;
import nta.datum.Datum;
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
  private Tuple finalizedAggTuple = null;
  private Tuple aggTuple = null;
  private boolean finalized = false;

  public SortAggregateExec(SubqueryContext ctx, GroupbyNode annotation,
                           PhysicalExec subOp) throws IOException {
    super(ctx, annotation, subOp);
  }

  @Override
  public Tuple next() throws IOException {
    Tuple keyTuple;
    Tuple tuple;
    boolean keyChange = false;
    while(!keyChange && !ctx.isStopped() && (tuple = subOp.next()) != null) {
      keyTuple = new VTuple(keylist.length);
      // build one key tuple
      for(int i = 0; i < keylist.length; i++) {
        keyTuple.put(i, tuple.get(keylist[i]));
      }

      if (prevKey == null) {
        prevKey = keyTuple;

        this.aggTuple = new VTuple(outputSchema.getColumnNum());
        for(int i = 0; i < outputSchema.getColumnNum(); i++) {
          Datum datum =
              evals[i].eval(inputSchema, tuple);
          this.aggTuple.put(i, datum);
        }
        finalized = false;
      } else {
        if (!prevKey.equals(keyTuple)) {
          // finalize aggregate and return
          finalizedAggTuple = this.aggTuple;
          finalized = true;
          this.aggTuple = new VTuple(outputSchema.getColumnNum());
          for(int i = 0; i < outputSchema.getColumnNum(); i++) {
            Datum datum =
                evals[i].eval(inputSchema, tuple);
            this.aggTuple.put(i, datum);
          }
          keyChange = true;
          prevKey = keyTuple;
        } else {
          // aggregate
          for(int i = 0; i < measurelist.length; i++) {
            Datum datum =
                evals[measurelist[i]].eval(inputSchema, tuple,
                    aggTuple.get(measurelist[i]));
            aggTuple.put(measurelist[i], datum);
          }
          finalized = false;
        }
      }
    }

    if (!finalized) {
      finalizedAggTuple = null;
      finalized = true;
      return this.aggTuple;
    }

    return finalizedAggTuple;
  }

  @Override
  public Schema getSchema() {
    return outputSchema;
  }

  @Override
  public void rescan() throws IOException {
    prevKey = null;
    subOp.rescan();
  }
}
