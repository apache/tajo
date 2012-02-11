/**
 * 
 */
package nta.engine.planner.physical;

import nta.storage.Tuple;
import nta.storage.VTuple;

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
    return keyTuple.hashCode() % numPartitions;
  }
}
