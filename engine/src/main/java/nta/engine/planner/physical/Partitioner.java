/**
 * 
 */
package nta.engine.planner.physical;

import nta.storage.Tuple;

import com.google.common.base.Preconditions;

/**
 * @author Hyunsik Choi
 */
public abstract class Partitioner {
  protected final int [] partitionKeys;
  protected final int numPartitions;
  
  public Partitioner(final int [] keyList, final int numPartitions) {
    Preconditions.checkArgument(keyList != null, 
        "Partition keys must be given");
    Preconditions.checkArgument(keyList.length > 0,
        "At least one partition key must be specified.");
    Preconditions.checkArgument(numPartitions > 0, 
        "The number of partitions must be positive: %s", numPartitions);
    this.partitionKeys = keyList;
    this.numPartitions = numPartitions;    
  }
  
  public abstract int getPartition(Tuple tuple);
}
