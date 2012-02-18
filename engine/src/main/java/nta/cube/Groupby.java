package nta.cube;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.engine.exec.eval.EvalNode;
import nta.engine.parser.QueryBlock.Target;
import nta.engine.planner.logical.GroupbyNode;
import nta.storage.Tuple;
import nta.storage.VTuple;

/* Groupby 실행 */
public class Groupby {

  private java.util.Map<Tuple, Tuple> tupleSlots;
  private Iterator<Entry<Tuple, Tuple>> iterator = null;

  /* LocalEngn용 execute */
  public void execute(CubeConf conf, LinkedList<Row> inputrowlist,
      LinkedList<KVpair> outputKVlist) {

    GroupbyNode gnode = Cons.gnode;
    Schema outschema = new Schema(gnode.getOutputSchema());
    outschema.addColumn("count", DataType.INT);

    tupleSlots = new HashMap<Tuple, Tuple>(10000);

    int[] groupf = new int[gnode.getGroupingColumns().length];
    int[] measuref = new int[gnode.getTargetList().length
        - gnode.getGroupingColumns().length];

    int i = 0;
    for (Column col : gnode.getGroupingColumns()) {
      groupf[i] = gnode.getInputSchema().getColumnId(col.getQualifiedName());
      i++;
    }

    i = 0;
    search: for (int z = 0; z < gnode.getTargetList().length; z++) {
      for (int t : groupf) {
        if (t == z) {
          continue search;
        }
      }
      measuref[i] = z;
      i++;
    }

    i = 0;
    EvalNode[] evals = new EvalNode[gnode.getTargetList().length];
    for (Target t : gnode.getTargetList()) {
      evals[i] = t.getEvalTree();
      i++;
    }

    for (Row row : inputrowlist) {
      Tuple keyTuple = null;
      keyTuple = new VTuple(groupf.length);
      for (int z = 0; z < groupf.length; z++) {
        keyTuple.put(z,
            evals[groupf[z]].eval(gnode.getInputSchema(), row));
      }

      if (tupleSlots.containsKey(keyTuple)) { // if key found
        Tuple tmpTuple = tupleSlots.get(keyTuple);
        for (int z = 0; z < measuref.length; z++) {
          Datum datum = evals[measuref[z]].eval(gnode.getInputSchema(), 
              row, tmpTuple.get(measuref[z]));
          tmpTuple.put(measuref[z], datum);
          tmpTuple.put(
              outschema.getColumnId("count"),
              tmpTuple.get(outschema.getColumnId("count")).plus(
                  DatumFactory.createInt(row.count)));
          tupleSlots.put(keyTuple, tmpTuple);
        }
      } else { // if the key occurs firstly
        VTuple tuple = new VTuple(outschema.getColumnNum());
        for (int z = 0; z < gnode.getOutputSchema().getColumnNum(); z++) {
          Datum datum = evals[z].eval(gnode.getInputSchema(), row);
          tuple.put(z, datum);
        }
        tuple.put(gnode.getOutputSchema().getColumnNum(),
            DatumFactory.createInt(row.count));
        tupleSlots.put(keyTuple, tuple);
      }
    }

    iterator = tupleSlots.entrySet().iterator();
    KVpair kvpair;
    while (iterator.hasNext()) {
      kvpair = new KVpair();
      Entry<Tuple, Tuple> hashmap = iterator.next();
      for (int z = 0; z < groupf.length; z++) {
        kvpair.key[z] = hashmap.getValue().get(groupf[z]);
      }
      for (int z = 0; z < measuref.length; z++) {
        kvpair.val[z] = hashmap.getValue().get(measuref[z]);
      }
      kvpair.count = hashmap.getValue()
          .getInt(gnode.getOutputSchema().getColumnNum()).asInt();
      outputKVlist.add(kvpair);
    }
  }

  /* ServerEngn용 execute */
  public void execute2(CubeConf conf, LinkedList<Row> inputrowlist,
      LinkedList<KVpair> outputKVlist) {

    GroupbyNode gnode = Cons.gnode;
    Schema outschema = new Schema(gnode.getOutputSchema());
    outschema.addColumn("count", DataType.INT);

    tupleSlots = new HashMap<Tuple, Tuple>(1000);

    int[] groupf = new int[gnode.getGroupingColumns().length];
    int[] measuref = new int[gnode.getTargetList().length
        - gnode.getGroupingColumns().length];

    int i = 0;
    for (Column col : gnode.getGroupingColumns()) {
      groupf[i] = gnode.getInputSchema().getColumnId(col.getQualifiedName());
      i++;
    }

    i = 0;
    search: for (int z = 0; z < gnode.getTargetList().length; z++) {
      for (int t : groupf) {
        if (t == z) {
          continue search;
        }
      }
      measuref[i] = z;
      i++;
    }

    for (Row row : inputrowlist) {
      Tuple keyTuple = null;
      keyTuple = new VTuple(groupf.length);
      for (int z = 0; z < groupf.length; z++) {
        keyTuple.put(z, row.values[groupf[z]]);
      }

      if (tupleSlots.containsKey(keyTuple)) {
        Tuple tmpTuple = tupleSlots.get(keyTuple);
        for (int z = 0; z < measuref.length; z++) {
          Datum datum = row.values[measuref[z]];
          tmpTuple.put(measuref[z], datum);
          tmpTuple.put(
              outschema.getColumnId("count"),
              tmpTuple.get(outschema.getColumnId("count")).plus(
                  DatumFactory.createInt(row.count)));
          tupleSlots.put(keyTuple, tmpTuple);
        }
      } else { // if the key occurs firstly
        VTuple tuple = new VTuple(outschema.getColumnNum());
        for (int z = 0; z < gnode.getOutputSchema().getColumnNum(); z++) {
          Datum datum = row.values[z];
          tuple.put(z, datum);
        }
        tuple.put(gnode.getOutputSchema().getColumnNum(),
            DatumFactory.createInt(row.count));
        tupleSlots.put(keyTuple, tuple);
      }
    }

    iterator = tupleSlots.entrySet().iterator();
    KVpair kvpair;
    while (iterator.hasNext()) {
      kvpair = new KVpair();
      Entry<Tuple, Tuple> hashmap = iterator.next();
      for (int z = 0; z < groupf.length; z++) {
        kvpair.key[z] = hashmap.getValue().get(groupf[z]);
      }
      for (int z = 0; z < measuref.length; z++) {
        kvpair.val[z] = hashmap.getValue().get(measuref[z]);
      }
      kvpair.count = hashmap.getValue()
          .getInt(gnode.getOutputSchema().getColumnNum()).asInt();
      outputKVlist.add(kvpair);
    }
  }
}
