package nta.engine.planner.physical;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.engine.SubqueryContext;
import nta.engine.exec.eval.EvalNode;
import nta.engine.planner.PlannerUtil;
import nta.engine.planner.logical.JoinNode;
import nta.engine.utils.TupleUtil;
import nta.storage.FrameTuple;
import nta.storage.Tuple;
import nta.storage.VTuple;

public class HashJoinExec extends PhysicalExec {
  // from logical plan
  private Schema inSchema;
  private Schema outSchema;
  private EvalNode joinQual;

  // sub operations
  private PhysicalExec outer;
  private PhysicalExec inner;

  private JoinNode ann;

  private List<Column[]> keylist;

  public PhysicalExec getouter() {
    return this.outer;
  }

  public PhysicalExec getinner() {
    return this.inner;
  }

  public JoinNode getJoinNode() {
    return this.ann;
  }

  // temporal tuples and states for nested loop join
  private FrameTuple frameTuple;
  private Tuple outputTuple = null;
  private Map<Tuple, List<Tuple>> tupleSlots;
  private Iterator<Tuple> iterator = null;

  // projection
  private final int[] targetIds;

  public HashJoinExec(SubqueryContext ctx, JoinNode ann, PhysicalExec outer,
      PhysicalExec inner) {
    this.outer = outer;
    this.inner = inner;
    this.inSchema = ann.getInputSchema();
    this.outSchema = ann.getOutputSchema();
    this.joinQual = ann.getJoinQual();
    this.ann = ann;
    this.tupleSlots = new HashMap<Tuple, List<Tuple>>(1000);

    Column[] col = PlannerUtil.getJoinKeyPairs(joinQual,
        outer.getSchema(), inner.getSchema()).get(0);
    this.keylist = new ArrayList<Column[]>();

    for (int idx = 0; idx < col.length; idx++) {
      keylist.add(PlannerUtil.getJoinKeyPairs(joinQual,
          outer.getSchema(), inner.getSchema()).get(idx));
    }

    // for projection
    targetIds = TupleUtil.getTargetIds(inSchema, outSchema);

    // for join
    frameTuple = new FrameTuple();
    outputTuple = new VTuple(outSchema.getColumnNum());
  }

  public Tuple next() throws IOException {
    Tuple t = null;
    Tuple keyTuple = null;
    if (tupleSlots.isEmpty()) {
      while ((t = inner.next()) != null) {
        keyTuple = new VTuple(keylist.size());
        List<Tuple> newvalue = null;
        for (int i = 0; i < keylist.size(); i++) {
          keyTuple.put(i, t.get(inner.getSchema().getColumnId(keylist.get(i)[1].getQualifiedName())));
        }
        if (tupleSlots.containsKey(keyTuple)) {
          newvalue = tupleSlots.get(keyTuple);
          newvalue.add(t);
          tupleSlots.put(keyTuple, newvalue);
        } else {
          newvalue = new ArrayList<Tuple>();
          newvalue.add(t);
          tupleSlots.put(keyTuple, newvalue);
        }
      }
    }
    
    keyTuple = new VTuple(keylist.size());
    while (true) {
      if (iterator == null || !iterator.hasNext()) {
        t = outer.next();
        if (t == null) {
          return null;
        }
        for (int i = 0; i < keylist.size(); i++) {
          keyTuple.put(i, t.get(outer.getSchema().getColumnId(keylist.get(i)[0].getQualifiedName())));
        }
      }

      if (tupleSlots.containsKey(keyTuple)) {
        iterator = tupleSlots.get(keyTuple).iterator();
        frameTuple.set(t, iterator.next());
        TupleUtil.project(frameTuple, outputTuple, targetIds);
        return outputTuple;
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
    iterator = null;
    tupleSlots.clear();
    keylist.clear();
  }
}
