package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

import java.util.HashSet;

public class TupleSet extends HashSet<Tuple> {

  @Override
  public boolean add(Tuple tuple) {
    return super.add(new VTuple(tuple));
  }
}
