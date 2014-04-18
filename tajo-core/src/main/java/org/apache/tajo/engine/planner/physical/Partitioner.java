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

import com.google.common.base.Preconditions;
import org.apache.tajo.storage.Tuple;

public abstract class Partitioner {
  protected final int [] partitionKeyIds;
  protected final int numPartitions;
  
  public Partitioner(final int [] keyList, final int numPartitions) {
    Preconditions.checkArgument(keyList != null, 
        "Partition keys must be given");
    Preconditions.checkArgument(keyList.length >= 0,
        "At least one partition key must be specified.");
    // In outer join, zero can be passed into this value because of empty tables.
    // So, we should allow zero.
    Preconditions.checkArgument(numPartitions >= 0,
        "The number of partitions must be positive: %s", numPartitions);
    this.partitionKeyIds = keyList;
    this.numPartitions = numPartitions;    
  }
  
  public abstract int getPartition(Tuple tuple);
}
