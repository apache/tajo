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

package org.apache.tajo.plan.logical;

import java.util.Arrays;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.Expose;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.util.StringUtils;
import org.apache.tajo.util.TUtil;

import static org.apache.tajo.plan.serder.PlanProto.ShuffleType;

/**
 * ShuffeFileWriteNode is an expression for an intermediate data materialization step.
 */
public class ShuffleFileWriteNode extends PersistentStoreNode implements Cloneable {
  @Expose private ShuffleType shuffleType = ShuffleType.NONE_SHUFFLE;
  @Expose private int numOutputs;
  @Expose private Column [] shuffleKeys;

  public ShuffleFileWriteNode(int pid) {
    super(pid, NodeType.STORE);
  }
    
  public final int getNumOutputs() {
    return this.numOutputs;
  }
  
  public final boolean hasShuffleKeys() {
    return this.shuffleKeys != null;
  }
  
  public final Column [] getShuffleKeys() {
    return shuffleKeys;
  }
  
  public final void setShuffle(ShuffleType type, Column[] keys, int numPartitions) {
    Preconditions.checkArgument(keys.length >= 0, 
        "At least one partition key must be specified.");
    // In outer join, zero can be passed into this value because of empty tables.
    // So, we should allow zero.
    Preconditions.checkArgument(numPartitions >= 0,
        "The number of partitions must be positive: %s", numPartitions);

    this.shuffleType = type;
    this.shuffleKeys = keys;
    this.numOutputs = numPartitions;
  }

  public ShuffleType getShuffleType() {
    return this.shuffleType;
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + numOutputs;
    result = prime * result + Arrays.hashCode(shuffleKeys);
    result = prime * result + ((shuffleType == null) ? 0 : shuffleType.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ShuffleFileWriteNode) {
      ShuffleFileWriteNode other = (ShuffleFileWriteNode) obj;
      boolean eq = super.equals(other);
      eq = eq && this.numOutputs == other.numOutputs;
      eq = eq && TUtil.checkEquals(shuffleKeys, other.shuffleKeys);
      return eq;
    } else {
      return false;
    }
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    ShuffleFileWriteNode store = (ShuffleFileWriteNode) super.clone();
    store.numOutputs = numOutputs;
    store.shuffleKeys = shuffleKeys != null ? shuffleKeys.clone() : null;
    return store;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("Shuffle Write (type=" + shuffleType.name().toLowerCase());
    if (storageType != null) {
      sb.append(", storage="+ storageType);
    }
    sb.append(", part number=").append(numOutputs);
    if (shuffleKeys != null) {
      sb.append(", keys: ").append(StringUtils.join(shuffleKeys));
    }
    sb.append(")");
    
    return sb.toString();
  }
}