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

package org.apache.tajo.engine.planner.logical;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.util.TUtil;

import static org.apache.tajo.ipc.TajoWorkerProtocol.ShuffleType.NONE_SHUFFLE;

/**
 * ShuffeFileWriteNode is an expression for an intermediate data materialization step.
 */
public class ShuffleFileWriteNode extends PersistentStoreNode implements Cloneable {
  @Expose private TajoWorkerProtocol.ShuffleType shuffleType = NONE_SHUFFLE;
  @Expose private int numOutputs;
  @Expose private Column [] shuffleKeys;

  public ShuffleFileWriteNode(int pid, String tableName) {
    super(pid, tableName);
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
  
  public final void setShuffle(TajoWorkerProtocol.ShuffleType type, Column[] keys, int numPartitions) {
    Preconditions.checkArgument(keys.length >= 0, 
        "At least one partition key must be specified.");
    Preconditions.checkArgument(numPartitions > 0,
        "The number of partitions must be positive: %s", numPartitions);

    this.shuffleType = type;
    this.shuffleKeys = keys;
    this.numOutputs = numPartitions;
  }

  public TajoWorkerProtocol.ShuffleType getShuffleType() {
    return this.shuffleType;
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
    StringBuilder sb = new StringBuilder();
    sb.append("\"Store\": {\"table\": \""+tableName);
    if (storageType != null) {
      sb.append(", storage: "+ storageType.name());
    }
    sb.append(", partnum: ").append(numOutputs).append("}")
    .append(", ");
    if (shuffleKeys != null) {
      sb.append("\"partition keys: [");
      for (int i = 0; i < shuffleKeys.length; i++) {
        sb.append(shuffleKeys[i]);
        if (i < shuffleKeys.length - 1)
          sb.append(",");
      }
      sb.append("],");
    }
    
    sb.append("\n  \"out schema\": ").append(getOutSchema()).append(",")
    .append("\n  \"in schema\": ").append(getInSchema());

    sb.append("}");
    
    return sb.toString() + "\n" + getChild().toString();
  }
}