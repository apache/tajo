/**
 * 
 */
package nta.engine.planner.physical;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.annotations.VisibleForTesting;
import nta.catalog.Schema;
import nta.datum.Datum;
import nta.engine.SubqueryContext;
import nta.engine.planner.logical.GroupbyNode;
import nta.storage.Tuple;
import nta.storage.VTuple;

/**
 * This is the hash-based GroupBy Operator.
 * 
 * @author Hyunsik Choi
 */
public class HashAggregateExec extends AggregationExec {
  private Tuple tuple = null;
  private Map<Tuple, Tuple> tupleSlots;
  private boolean computed = false;
  private Iterator<Entry<Tuple, Tuple>> iterator = null;

  /**
   * @throws IOException 
	 * 
	 */
  public HashAggregateExec(SubqueryContext ctx, GroupbyNode annotation,
                           PhysicalExec subOp) throws IOException {
    super(ctx, annotation, subOp);
    tupleSlots = new HashMap<Tuple, Tuple>(1000);
    this.tuple = new VTuple(outputSchema.getColumnNum());
  }

  @VisibleForTesting
  public PhysicalExec getSubOp() {
    return this.subOp;
  }

  @VisibleForTesting
  public GroupbyNode getAnnotation() {
    return this.annotation;
  }

  @VisibleForTesting
  public void setSubOp(PhysicalExec subOp) {
    this.subOp = subOp;
  }
  
  private void compute() throws IOException {
    Tuple tuple;
    Tuple keyTuple;
    while((tuple = subOp.next()) != null && !ctx.isStopped()) {
      keyTuple = new VTuple(keylist.length);
      // build one key tuple
      for(int i = 0; i < keylist.length; i++) {
        keyTuple.put(i, tuple.get(keylist[i]));
      }
      
      if(tupleSlots.containsKey(keyTuple)) {
        Tuple tmpTuple = tupleSlots.get(keyTuple);
        for(int i = 0; i < measurelist.length; i++) {
          evals[measurelist[i]].eval(evalContexts[i], inputSchema, tuple,
                tmpTuple.get(measurelist[i]));
          Datum datum = evals[measurelist[i]].terminate(evalContexts[i]);
          tmpTuple.put(measurelist[i], datum);
          tupleSlots.put(keyTuple, tmpTuple);
        }
      } else { // if the key occurs firstly
        this.tuple = new VTuple(outputSchema.getColumnNum());
        for(int i = 0; i < outputSchema.getColumnNum(); i++) {
          evals[i].eval(evalContexts[i], inputSchema, tuple);
          Datum datum = evals[i].terminate(evalContexts[i]);
          this.tuple.put(i, datum);
        }
        tupleSlots.put(keyTuple, this.tuple);
      }
    }
  }

  @Override
  public Tuple next() throws IOException {
    if(!computed) {
      compute();
      iterator = tupleSlots.entrySet().iterator();
      computed = true;
    }
        
    if(iterator.hasNext()) {
      return iterator.next().getValue();
    } else {
      return null;
    }
  }

  @Override
  public Schema getSchema() {
    return outputSchema;
  }

  @Override
  public void rescan() throws IOException {    
    iterator = tupleSlots.entrySet().iterator();
    computed = true;
  }
}
