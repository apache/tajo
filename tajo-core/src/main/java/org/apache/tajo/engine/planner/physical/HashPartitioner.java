/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

public class HashPartitioner extends Partitioner {
  private final Tuple keyTuple;
  
  public HashPartitioner(final int [] keys, final int numPartitions) {
    super(keys, numPartitions);
    this.keyTuple = new VTuple(partitionKeyIds.length);
  }
  
  @Override
  public int getPartition(Tuple tuple) {
    // In outer join, partition number can be zero because of empty tables.
    // So, we should return zero for this case.
    if (numPartitions == 0)
      return 0;

    // build one key tuple
    for (int i = 0; i < partitionKeyIds.length; i++) {
      keyTuple.put(i, tuple.get(partitionKeyIds[i]));
    }
    return (keyTuple.hashCode() & Integer.MAX_VALUE) % numPartitions;
  }
}
