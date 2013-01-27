/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
