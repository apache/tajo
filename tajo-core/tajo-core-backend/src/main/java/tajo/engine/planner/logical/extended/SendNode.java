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
package tajo.engine.planner.logical.extended;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import tajo.catalog.Column;
import tajo.engine.json.GsonCreator;
import tajo.engine.planner.logical.ExprType;
import tajo.engine.planner.logical.LogicalNode;
import tajo.engine.planner.logical.UnaryNode;
import tajo.util.TUtil;

import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This logical node means that the worker sends intermediate data to 
 * some destined one or more workers.
 */
public class SendNode extends UnaryNode {
  @Expose private PipeType pipeType;
  @Expose private RepartitionType repaType;
  /** This will be used for pipeType == PUSH. */
  @Expose private Map<Integer, URI> destURIs;
  @Expose private Column[] partitionKeys;
  @Expose private int numPartitions;

  private SendNode() {
    super(ExprType.SEND);
  }
  
  public SendNode(PipeType pipeType, RepartitionType repaType) {
    this();
    this.pipeType = pipeType;
    this.repaType = repaType;
    this.destURIs = Maps.newHashMap();
  }

  public PipeType getPipeType() {
    return this.pipeType;
  }
  
  public RepartitionType getRepartitionType() {
    return this.repaType;
  }
  
  public URI getDestURI(int partition) {
    return this.destURIs.get(partition);
  }
  
  public void setPartitionKeys(Column [] keys, int numPartitions) {
    Preconditions.checkState(repaType != RepartitionType.NONE,
        "Hash or Sort repartition only requires the partition keys");
    Preconditions.checkArgument(keys.length > 0, 
        "At least one partition key must be specified.");
    Preconditions.checkArgument(numPartitions > 0,
        "The number of partitions must be positive: %s", numPartitions);
    this.partitionKeys = keys;
    this.numPartitions = numPartitions;
  }
  
  public boolean hasPartitionKeys() {
    return this.partitionKeys != null;
  }
  
  public Column [] getPartitionKeys() {
    return this.partitionKeys;
  }
  
  public int getPartitionsNum() {
    return this.numPartitions;
  }
  
  public Iterator<Entry<Integer, URI>> getAllDestURIs() {
    return this.destURIs.entrySet().iterator();
  }
  
  public void putDestURI(int partition, URI uri) {
    this.destURIs.put(partition, uri);
  }
  
  public void setDestURIs(Map<Integer, URI> destURIs) {
    this.destURIs = destURIs;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SendNode) {
      SendNode other = (SendNode) obj;
      return pipeType == other.pipeType
          && repaType == other.repaType
          && TUtil.checkEquals(destURIs, other.destURIs);
    } else {
      return false;
    }
  }
  
  @Override
  public int hashCode() {
    return Objects.hashCode(pipeType, repaType, destURIs);
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    SendNode send = (SendNode) super.clone();
    send.pipeType = pipeType;
    send.repaType = repaType;
    send.destURIs = Maps.newHashMap();
    for (Entry<Integer, URI> entry : destURIs.entrySet()) {
      send.destURIs.put(entry.getKey(), entry.getValue());
    }
    
    return send;
  }

  @Override
  public String toString() {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    return gson.toJson(this);
  }

  @Override
  public String toJSON() {
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
}