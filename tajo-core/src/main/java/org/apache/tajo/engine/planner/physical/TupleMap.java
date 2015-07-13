package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.annotation.NotNull;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

import java.util.HashMap;

public class TupleMap<E> extends HashMap<Tuple, E> {

  private static final int DEFAULT_INIT_CAPACITY = 100000;

  private final Tuple keyTuple;
  private final int keyIds[];

  public TupleMap(Schema inSchema, Column[] keyColumns) {
    this(inSchema, keyColumns, DEFAULT_INIT_CAPACITY);
  }

  public TupleMap(Schema inSchema, Column[] keyColumns, int initialCapacity) {
    super(initialCapacity);
    keyTuple = new VTuple(keyColumns.length);
    keyIds = new int[keyColumns.length];
    for (int i = 0; i < keyColumns.length; i++) {
      keyIds[i] = inSchema.getColumnId(keyColumns[i].getQualifiedName());
    }
  }

  public TupleMap(TupleMap tupleMap){
    super(tupleMap);
    keyTuple = new VTuple(tupleMap.keyTuple.size());
    keyIds = new int[tupleMap.keyIds.length];
    System.arraycopy(tupleMap.keyIds, 0, this.keyIds, 0, this.keyIds.length);
  }

  @Override
  public E put(@Nullable Tuple key, E value) {
    if (key != null) {
      return super.put(new VTuple(key), value);
    } else {
      return super.put(null, value);
    }
  }

  public E putWithoutKeyCopy(@Nullable Tuple key, E value) {
    return super.put(key, value);
  }

  public Tuple getKey(Tuple tuple) {
    keyTuple.clear();
    for (int i = 0; i < keyIds.length; i++) {
      keyTuple.put(i, tuple.asDatum(keyIds[i]));
    }
    return keyTuple;
  }

  public E getWithImplicitKeyConversion(Tuple tuple) {
    return this.get(this.getKey(tuple));
  }
}
