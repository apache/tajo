/**
 * 
 */
package nta.engine.planner.physical;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.datum.Datum;
import nta.engine.SubqueryContext;
import nta.engine.exec.eval.EvalNode;
import nta.engine.parser.QueryBlock.Target;
import nta.engine.planner.logical.GroupbyNode;
import nta.storage.Tuple;
import nta.storage.VTuple;

/**
 * This class is the hash-based GroupBy Operator.
 * 
 * @author Hyunsik Choi
 * 
 */
public class GroupByExec extends PhysicalExec {
  private final SubqueryContext ctx;
  private PhysicalExec subOp = null;
  
  @SuppressWarnings("unused")
  private final EvalNode havingQual;
  private final Schema inputSchema;
  private final Schema outputSchema;
  private final EvalNode evals [];
  private Tuple tuple = null;
  private int keylist [];
  private int measurelist [];
  private Map<Tuple, Tuple> tupleSlots;
  
  private boolean computed = false;
  private Iterator<Entry<Tuple, Tuple>> iterator = null;

  /**
   * @throws IOException 
	 * 
	 */
  public GroupByExec(SubqueryContext ctx, GroupbyNode annotation, 
      PhysicalExec subOp) throws IOException {
    this.ctx = ctx;
    this.subOp = subOp;
    this.havingQual = annotation.getHavingCondition();
    this.inputSchema = annotation.getInputSchema();
    this.outputSchema = annotation.getOutputSchema();
    
    tupleSlots = new HashMap<Tuple, Tuple>(1000);

    // getting key list
    keylist = new int[annotation.getGroupingColumns().length];
    int idx = 0;
    for (Column col : annotation.getGroupingColumns()) {
      keylist[idx] = inputSchema.getColumnId(col.getQualifiedName());
      idx++;
    }
    
    // getting value list
    int valueIdx = 0;
    measurelist = new int[annotation.getTargetList().length - keylist.length];
    if (measurelist.length > 0) {
      search: for (int inputIdx = 0; inputIdx < annotation.getTargetList().length; inputIdx++) {
        for (int key : keylist) { // eliminate key field
          if (annotation.getTargetList()[inputIdx].getColumnSchema().getColumnName()
              .equals(inputSchema.getColumn(key).getColumnName())) {
            continue search;
          }
        }
        measurelist[valueIdx] = inputIdx;
        valueIdx++;
      }
    }
    
    idx = 0;
    evals = new EvalNode[annotation.getTargetList().length];
    for (Target t : annotation.getTargetList()) {
      evals[idx] = t.getEvalTree();
      idx++;
    }
    
    this.tuple = new VTuple(outputSchema.getColumnNum());
  }
  
  private void compute() throws IOException {
    Tuple tuple = null;
    Tuple keyTuple = null;
    while((tuple = subOp.next()) != null && !ctx.isStopped()) {
      keyTuple = new VTuple(keylist.length);
      // build one key tuple
      for(int i = 0; i < keylist.length; i++) {
        keyTuple.put(i, tuple.get(keylist[i]));
      }
      
      if(tupleSlots.containsKey(keyTuple)) {
        Tuple tmpTuple = tupleSlots.get(keyTuple);
        for(int i = 0; i < measurelist.length; i++) {
          Datum datum =
            evals[measurelist[i]].eval(inputSchema, tuple, 
                tmpTuple.get(measurelist[i]));
          tmpTuple.put(measurelist[i], datum);
          tupleSlots.put(keyTuple, tmpTuple);
        }
      } else { // if the key occurs firstly
        this.tuple = new VTuple(outputSchema.getColumnNum());
        for(int i = 0; i < outputSchema.getColumnNum(); i++) {
          Datum datum =
              evals[i].eval(inputSchema, tuple);
          
          this.tuple.put(i, datum);
        }
        tupleSlots.put(keyTuple, this.tuple);
      }
    }
  }

  @Override
  public Tuple next() throws IOException {
    if(computed == false) {
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
