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
package org.apache.tajo.engine.planner.logical.extended;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import org.apache.tajo.engine.json.GsonCreator;
import org.apache.tajo.engine.planner.logical.ExprType;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.LogicalNodeVisitor;

import java.net.URI;
import java.util.*;
import java.util.Map.Entry;

public final class ReceiveNode extends LogicalNode implements Cloneable {
  @Expose private PipeType pipeType;
  @Expose private RepartitionType repaType;
  @Expose private Map<String, List<URI>> fetchMap;

  private ReceiveNode() {
    super(ExprType.RECEIVE);
  }
  public ReceiveNode(PipeType pipeType, RepartitionType shuffleType) {
    this();
    this.pipeType = pipeType;
    this.repaType = shuffleType;
    this.fetchMap = Maps.newHashMap();
  }

  public PipeType getPipeType() {
    return this.pipeType;
  }

  public RepartitionType getRepartitionType() {
    return this.repaType;
  }
  
  public void addData(String name, URI uri) {
    if (fetchMap.containsKey(name)) {
      fetchMap.get(name).add(uri);
    } else {
      fetchMap.put(name, Lists.newArrayList(uri));
    }
  }
  
  public Collection<URI> getSrcURIs(String name) {
    return Collections.unmodifiableList(fetchMap.get(name));
  }

  public Collection<Entry<String, List<URI>>> getAllDataSet() {
    return Collections.unmodifiableSet(fetchMap.entrySet());
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ReceiveNode) {
      ReceiveNode other = (ReceiveNode) obj;
      return pipeType == other.pipeType && repaType == other.repaType;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(pipeType, repaType);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    ReceiveNode receive = (ReceiveNode) super.clone();
    receive.pipeType = pipeType;
    receive.repaType = repaType;
    receive.fetchMap = Maps.newHashMap();
    // Both String and URI are immutable, but a list is mutable.
    for (Entry<String, List<URI>> entry : fetchMap.entrySet()) {
      receive.fetchMap
          .put(entry.getKey(), new ArrayList<URI>(entry.getValue()));
    }

    return receive;
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

  @Override
  public void preOrder(LogicalNodeVisitor visitor) {
    visitor.visit(this);
  }
  
  @Override
  public void postOrder(LogicalNodeVisitor visitor) {
    visitor.visit(this);
  }
}
