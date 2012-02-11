/**
 * 
 */
package nta.engine.planner.physical;

import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public abstract class Partitioner {
  protected final int [] partitionKeys;
  protected final int numPartitions;
  
  public Partitioner(final int [] keyList, final int numPartitions) {
    this.partitionKeys = keyList;
    this.numPartitions = numPartitions;    
  }
  
  public abstract int getPartition(Tuple tuple);
}
