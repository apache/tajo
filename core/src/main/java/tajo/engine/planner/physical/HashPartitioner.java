/**
 * 
 */
package tajo.engine.planner.physical;

import tajo.storage.Tuple;
import tajo.storage.VTuple;

/**
 * @author Hyunsik Choi
 */
public class HashPartitioner extends Partitioner {
  private final Tuple keyTuple;
  
  public HashPartitioner(final int [] keys, final int numPartitions) {
    super(keys, numPartitions);
    this.keyTuple = new VTuple(partitionKeys.length);
  }
  
  @Override
  public int getPartition(Tuple tuple) {
    // build one key tuple
    for (int i = 0; i < partitionKeys.length; i++) {
      keyTuple.put(i, tuple.get(partitionKeys[i]));
    }
    return (keyTuple.hashCode() & Integer.MAX_VALUE) %
        (numPartitions == 32 ? numPartitions-1 : numPartitions);
  }
}
