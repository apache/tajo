package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

import java.util.ArrayList;

public class TupleList extends ArrayList<Tuple> {

  public TupleList() {
    super();
  }

  public TupleList(int initialCapacity) {
    super(initialCapacity);
  }

  @Override
  public boolean add(Tuple tuple) {
    return super.add(new VTuple(tuple));
  }
}
